from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id").isin([17, 20, 26]))
    .groupBy(
        F.when(F.col("s.browser").isin(["Safari", "Edge"]), "priority").otherwise("other").alias("browser_group"),
    )
    .agg(
        F.sum(F.when(F.col("we.page") == "/checkout", F.col("we.duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
    )
    .orderBy(F.col("checkout_duration_sec").desc())
)
