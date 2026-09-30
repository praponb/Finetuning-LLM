from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("is_new_session_tier").orderBy(F.col("avg_burst_id").desc())

flagged = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "HIGH").otherwise("LOW").alias("is_new_session_tier"),
        "browser",
        "burst_id",
    )
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.min("started_at").alias("started_at"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("safari_rows") > 1)
    .withColumn("avg_burst_id_dense_rank", F.dense_rank().over(w3))
    .orderBy("avg_burst_id")
)
