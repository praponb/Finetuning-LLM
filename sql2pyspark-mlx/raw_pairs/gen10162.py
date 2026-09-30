from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("country").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy(F.col("is_new_session").desc_nulls_last())
w_ordered = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy(F.col("days_since_started_at").desc(), "id")
w8 = Window.partitionBy("country").orderBy("is_new_session", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy(F.to_date("started_at")).orderBy("days_since_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("browser").orderBy(F.col("last4_avg_is_new_session").desc(), "id")
w11 = Window.partitionBy("device").orderBy(F.col("last4_avg_is_new_session").desc())

cleaned = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select(
        "*",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

with_windows = (
    islands
    .withColumn("prev_island_id", F.lag("island_id", 1).over(w3))
    .withColumn("running_days_since_started_at", F.sum("days_since_started_at").over(w4))
    .withColumn("is_new_session_percent_rank", F.percent_rank().over(w5))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_ordered))
    .withColumn("days_since_started_at_bucket", F.ntile(3).over(w7))
)

metrics = (
    with_windows
    .select(
        "id",
        "device",
        "browser",
        "running_days_since_started_at",
        "is_new_session_percent_rank",
        "last_known_browser",
        F.sum("is_new_session").over(w8).alias("last4_sum_is_new_session"),
        F.avg("is_new_session").over(w8).alias("last4_avg_is_new_session"),
        F.min("is_new_session").over(w8).alias("last4_min_is_new_session"),
        F.first("browser", ignorenulls=True).over(w9).alias("first_known_browser"),
    )
)

filtered = (
    metrics
    .filter(F.col("last4_sum_is_new_session") > 1.1)
    .select(
        "id",
        "device",
        "browser",
        "running_days_since_started_at",
        "is_new_session_percent_rank",
        "last4_sum_is_new_session",
        "last4_avg_is_new_session",
        F.when(F.col("device").isin(["mobile", "tablet"]), "Group A").otherwise("Other").alias("device_group"),
    )
)

scored = (
    filtered
    .withColumn("last4_avg_is_new_session_bucket", F.ntile(10).over(w10))
    .withColumn("last4_avg_is_new_session_cume_dist", F.cume_dist().over(w11))
)

kept = (
    scored
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < "2024-03-01"), "left_semi")
)

result = (
    kept
    .withColumn("last4_avg_is_new_session_bucket_tier", F.when(F.col("last4_avg_is_new_session_bucket") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("last4_sum_is_new_session_tier", F.when(F.col("last4_sum_is_new_session") >= 3.9, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["last4_avg_is_new_session_bucket_tier", "device", "last4_sum_is_new_session_tier"], ["last4_avg_is_new_session_bucket_tier", "device"]],
        "last4_avg_is_new_session_bucket_tier",
        "device",
        "last4_sum_is_new_session_tier",
    )
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.avg(F.when(F.col("device") == "mobile", F.col("is_new_session_percent_rank"))).alias("mobile_avg_is_new_session_percent_rank"),
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("device") == "mobile", F.col("is_new_session_percent_rank"))).alias("mobile_max_is_new_session_percent_rank"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("last4_avg_is_new_session_bucket")).otherwise(0)).alias("chrome_last4_avg_is_new_session_bucket"),
        F.grouping("last4_avg_is_new_session_bucket_tier").alias("grouping_id"),
    )
)
