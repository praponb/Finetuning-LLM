from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_grp = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("days_since_started_at", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("device").orderBy("days_since_started_at_count_7d", "id")
w8 = Window.orderBy("next_days_since_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("event_type").orderBy("started_at", "id")
w10 = Window.partitionBy("event_type").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("days_since_started_at_sum_90d").desc())
w12 = Window.partitionBy("event_type").orderBy(F.col("prev_started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-07-19 10:00:00").cast("timestamp"))
    .withColumn("country_browser_key", F.concat_ws("-", "country", "browser"))
)

valid = (
    base
    .filter((F.col("started_at") > F.lit("2024-02-12 21:00:00").cast("timestamp")) & F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "id",
        "device",
        "started_at",
        "country_browser_key",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.sum("days_since_started_at").over(w).alias("days_since_started_at_sum_90d"),
        F.lead("days_since_started_at", 1).over(w_grp).alias("next_days_since_started_at"),
        F.count("days_since_started_at").over(w3).alias("days_since_started_at_count_7d"),
        F.ntile(5).over(w4).alias("days_since_started_at_bucket"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

metrics_sessions = (
    latest_match
    .withColumn("days_since_started_at_sum_90d_count_7d", F.count("days_since_started_at_sum_90d").over(w6))
    .withColumn("next_days_since_started_at_count_7d", F.lead("days_since_started_at_count_7d", 1).over(w7))
    .withColumn("running_days_since_started_at_sum_90d", F.sum("days_since_started_at_sum_90d").over(w8))
)

prev_events = metrics_sessions.withColumn("prev_started_at", F.lag("started_at").over(w9))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w10))

with_windows = (
    sessions_sessions
    .withColumn("days_since_started_at_sum_90d_dense_rank", F.dense_rank().over(w11))
    .withColumn("running_days_since_started_at_sum_90d_max_7d", F.max("running_days_since_started_at_sum_90d").over(w12))
)

result = (
    with_windows
    .select(
        "id",
        "device",
        "country_browser_key",
        "days_since_started_at",
        "days_since_started_at_sum_90d",
        "next_days_since_started_at",
        "days_since_started_at_count_7d",
        "days_since_started_at_bucket",
        "days_since_started_at_sum_90d_count_7d",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "EXTENDED").when(F.col("device").rlike("^(mobile)$"), "CORE").otherwise("GROUP_A").alias("device_zone"),
    )
    .orderBy(F.col("device_zone").desc_nulls_last())
)
