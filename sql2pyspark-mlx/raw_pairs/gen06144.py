from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("session_id") >= 7, "HIGH").otherwise("LOW").alias("session_id_tier"),
    )
    .agg(
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
        F.min("started_at").alias("first_seen"),
    )
    .filter(F.col("avg_new_group_flag") > 0)
    .orderBy("avg_new_group_flag")
)
