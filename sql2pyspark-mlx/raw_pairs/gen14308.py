from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet"]) & (F.col("started_at") > F.to_timestamp(F.lit("2023-02-27 04:00:00"))))
    .withColumn("days_since_started_at", F.datediff(F.current_date(), "started_at"))
    .withColumn("country_filled", F.coalesce("country", F.lit("NONE")))
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy("device")
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
    )
    .filter(F.col("avg_session_id") < 1000000)
)
