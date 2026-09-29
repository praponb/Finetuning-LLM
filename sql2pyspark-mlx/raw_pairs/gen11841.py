from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("browser", F.date_trunc("week", "started_at").alias("week_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
        F.max("started_at").alias("last_started_at"),
        F.min("started_at").alias("started_at"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_rows"),
    )
    .orderBy("chrome_rows")
)
