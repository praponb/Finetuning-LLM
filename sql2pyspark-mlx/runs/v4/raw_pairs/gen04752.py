from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
    )
    .agg(
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.max(F.when(F.col("device") == "tablet", F.col("session_start_flag"))).alias("tablet_max_session_start_flag"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
    .filter(F.col("avg_session_start_flag") >= 0)
)
