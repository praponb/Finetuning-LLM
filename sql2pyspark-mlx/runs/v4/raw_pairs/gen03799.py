from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.date_trunc("WEEK", "started_at").alias("week_start"),
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
    )
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.countDistinct("device").alias("distinct_devices"),
        F.sum(F.when(F.col("device") == "tablet", F.col("session_id"))).alias("tablet_total_session_id"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("session_id_per_safari"),
    )
)
