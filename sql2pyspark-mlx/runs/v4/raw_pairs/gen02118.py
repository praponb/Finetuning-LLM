from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-08-28").cast("date")) & F.col("page").isNotNull())
    .withColumn("page_lower", F.lower("page"))
)

result = (
    base
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.country"), F.col("s.browser"), F.col("s.device"))
)
