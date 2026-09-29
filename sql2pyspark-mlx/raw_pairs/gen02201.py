from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("page")
w_ordered = Window.orderBy(F.col("event_time").desc(), "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy(F.col("duration_sec_filled_avg_5").desc(), "id")

base = (
    df_web_events
    .filter(F.col("event_time") > F.to_date(F.lit("2023-03-25")))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

metrics = (
    base
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "page_clean",
        F.avg("duration_sec_filled").over(w).alias("duration_sec_filled_avg_5"),
        F.stddev_samp("duration_sec_filled").over(w2).alias("page_stddev_samp_duration_sec_filled"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

gaps = (
    deduped
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec_filled_avg_5",
        "page_stddev_samp_duration_sec_filled",
        F.sum("is_new_burst").over(w5).alias("island_id"),
    )
)

ranked = (
    islands
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    ranked
    .select("id", "page")
    .subtract(
        df_web_events
        .filter(F.col("event_time") >= "2024-05-10")
        .select("id", "page")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

base_web_events = (
    reconciled
    .filter(~F.col("page").isin(["/search", "/product", "/home"]) & (~F.col("page").eqNullSafe(F.lit("/cart"))))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec_filled_avg_5",
        "page_stddev_samp_duration_sec_filled",
        "island_id",
        "rn",
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
    )
)

stats = (
    base_web_events
    .groupBy(
        F.when(F.col("page_stddev_samp_duration_sec_filled") >= 61, "HIGH").otherwise("LOW").alias("page_stddev_samp_duration_sec_filled_tier"),
        F.when(F.col("island_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.avg(F.when(F.col("page") == "/home", F.col("page_stddev_samp_duration_sec_filled"))).alias("home_avg_page_stddev_samp_duration_sec_filled"),
        F.size(F.collect_set("page")).alias("num_pages"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.countDistinct("page").alias("distinct_pages"),
    )
    .filter(F.col("distinct_pages") > 1)
)

result = (
    stats
    .select(
        "page_stddev_samp_duration_sec_filled_tier",
        "island_id_tier",
        "span_minutes",
        "page_list",
    )
    .orderBy("island_id_tier", F.col("span_minutes").desc())
)
