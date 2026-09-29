from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("week_start", F.date_trunc("WEEK", "prev_started_at"))
    .withColumn("activity_month", F.date_trunc("MONTH", "prev_started_at"))
    .rollup("week_start", "activity_month", "customer_id")
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.percentile_approx("burst_id", 0.95).alias("burst_id_p95"),
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("burst_id_per_de"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.min("prev_started_at").alias("first_seen"),
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
        F.grouping("week_start").alias("grouping_id"),
    )
    .orderBy("week_start")
)
