from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page", "session_id").orderBy(F.col("event_time").asc_nulls_first())
w3 = Window.partitionBy("event_type").orderBy(F.col("running_avg_duration_sec").desc(), "id")
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("session_id", "event_type").orderBy("running_avg_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("running_sum_duration_sec").desc_nulls_last())
w7 = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_web_events
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("running_sum_duration_sec"),
        F.avg("duration_sec").over(w).alias("running_avg_duration_sec"),
        F.max("duration_sec").over(w).alias("running_max_duration_sec"),
    )
)

valid = (
    scored
    .filter((F.col("event_time") >= F.lit("2023-04-09").cast("date")) & ~F.col("event_type").isin(["search", "click", "purchase"]))
    .select(
        "*",
        (F.col("duration_sec") - F.col("running_avg_duration_sec")).alias("duration_sec_minus_running_avg_duration_sec"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "running_sum_duration_sec",
        "running_avg_duration_sec",
    )
)

filtered = (
    latest
    .filter(F.col("event_time") >= F.lit("2023-10-14").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "running_sum_duration_sec",
        "running_avg_duration_sec",
        F.round("running_avg_duration_sec", 0).alias("running_avg_duration_sec_rounded"),
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)

enriched = (
    filtered
    .withColumn("running_avg_duration_sec_bucket", F.ntile(4).over(w3))
    .withColumn("running_avg_duration_sec_rounded_sum_90d", F.sum("running_avg_duration_sec_rounded").over(w4))
    .withColumn("first_known_running_avg_duration_sec", F.first("running_avg_duration_sec", ignorenulls=True).over(w5))
    .withColumn("running_sum_duration_sec_cume_dist", F.cume_dist().over(w6))
    .withColumn("cumulative_running_avg_duration_sec_rounded", F.sum("running_avg_duration_sec_rounded").over(w7))
)

daily = (
    enriched
    .groupBy(
        F.when(F.col("first_known_running_avg_duration_sec") >= 470, "HIGH").otherwise("LOW").alias("first_known_running_avg_duration_sec_tier"),
    )
    .agg(
        F.round(F.sum("running_avg_duration_sec_rounded_sum_90d") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("running_avg_duration_sec_rounded_sum_90d_per_add_to_cart"),
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.round(F.sum("cumulative_running_avg_duration_sec_rounded") / F.nullif(F.count(F.when(F.col("event_type") == "purchase", 1)), F.lit(0)), 2).alias("cumulative_running_avg_duration_sec_rounded_per_purchase"),
    )
    .filter(F.col("page_view_rows") < 1000)
)

result = (
    daily
    .select(
        "first_known_running_avg_duration_sec_tier",
        "running_avg_duration_sec_rounded_sum_90d_per_add_to_cart",
        "purchase_events",
        "page_view_rows",
        "cumulative_running_avg_duration_sec_rounded_per_purchase",
    )
    .orderBy(
        F.col("cumulative_running_avg_duration_sec_rounded_per_purchase").desc_nulls_first(),
    )
)
