from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device")
w2 = Window.orderBy("duration_sec", "id")
w3 = Window.partitionBy("device", "country")
w4 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("device_sum_duration_sec").asc_nulls_last())

filtered = (
    df_web_events
    .filter(F.col("event_type") != "page_view")
    .withColumn("event_time_period", F.date_trunc("week", "event_time"))
    .withColumn("event_type_lower", F.lower("event_type"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("DIRECT")))
)

enriched = (
    filtered
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isNotNull())
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.duration_sec",
        F.col("d.device"),
        F.col("d.country"),
        F.col("d.browser"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("device_sum_duration_sec"),
        F.lag("duration_sec", 1).over(w2).alias("prev_duration_sec"),
    )
)

scored_web_events = (
    scored
    .select(
        "id",
        "duration_sec",
        "country",
        "prev_duration_sec",
        F.count("device_sum_duration_sec").over(w3).alias("device_count_device_sum_duration_sec"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country"),
        F.percent_rank().over(w5).alias("device_sum_duration_sec_percent_rank"),
    )
)

base = (
    scored_web_events
    .filter(F.col("country").isNotNull() & (F.col("country").eqNullSafe(F.lit("UK"))))
    .withColumn("prev_duration_sec_rounded", F.round("prev_duration_sec", 2))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 397, "High").when(F.col("duration_sec") >= 189, "Medium").otherwise("Low"))
    .withColumn("country_filled", F.coalesce("country", F.lit("DIRECT")))
)

result = (
    base
    .withColumn("device_sum_duration_sec_percent_rank_tier", F.when(F.col("device_sum_duration_sec_percent_rank") >= 0.83, "LARGE").otherwise("SMALL"))
    .withColumn("device_count_device_sum_duration_sec_tier", F.when(F.col("device_count_device_sum_duration_sec") >= 16, "HIGH").otherwise("LOW"))
    .rollup(
        "device_sum_duration_sec_percent_rank_tier",
        "device_count_device_sum_duration_sec_tier",
    )
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.min("prev_duration_sec").alias("min_prev_duration_sec"),
        F.avg(F.when(F.col("country") == "TH", F.col("device_sum_duration_sec_percent_rank"))).alias("th_avg_device_sum_duration_sec_percent_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.grouping("device_sum_duration_sec_percent_rank_tier").alias("gid"),
    )
    .filter(F.col("th_avg_device_sum_duration_sec_percent_rank") > 0)
    .orderBy("th_avg_device_sum_duration_sec_percent_rank")
)
