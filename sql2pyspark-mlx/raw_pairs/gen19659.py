from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("session_id") == 34)
    .withColumn("page_lower", F.lower("page"))
)

summary = (
    filtered_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"), "session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
)

result = (
    summary
    .select("event_quarter", "session_id", "checkout_count")
    .orderBy(F.col("checkout_count").desc())
)
