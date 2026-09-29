from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("session_start_flag_tier"),
    )
    .agg(
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.percentile_approx("session_start_flag", 0.95).alias("session_start_flag_p95"),
        F.max("session_start_flag").alias("max_session_start_flag"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
)
