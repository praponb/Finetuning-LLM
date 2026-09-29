from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w4 = Window.partitionBy("page")
w5 = Window.orderBy(F.col("months_since_event_time").desc_nulls_first())
w6 = Window.partitionBy("event_type").orderBy(F.col("avg_duration_sec").desc())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_type") != "search"))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.upper("event_type").alias("event_type_upper"),
    )
)

enriched = (
    filtered
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("prev_duration_sec_2", F.lag("duration_sec", 2).over(w2))
)

cleaned = (
    enriched
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-07-12")))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "event_type_upper",
        "prev_duration_sec",
        "event_seq",
        "prev_duration_sec_2",
        (F.datediff(F.current_date(), "event_time") / 30).alias("months_since_event_time"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

remaining_keys = (
    cleaned
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("page") == "/home")
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("event_type") == "page_view")
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w3))
    .withColumn("page_max_prev_duration_sec_2", F.max("prev_duration_sec_2").over(w4))
    .withColumn("months_since_event_time_dense_rank", F.dense_rank().over(w5))
)

result = (
    metrics
    .groupBy(
        "event_type",
        F.when(F.col("page_max_prev_duration_sec_2") >= 509, "LARGE").otherwise("SMALL").alias("page_max_prev_duration_sec_2_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("prev_duration_sec", 0.5).alias("prev_duration_sec_p50"),
        F.count("*").alias("event_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("num_rows") < 1000)
    .withColumn("avg_duration_sec_rank", F.rank().over(w6))
)
