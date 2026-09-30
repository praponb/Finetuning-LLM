from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([9, 15, 32, 35]) & F.col("duration_sec").isNotNull() & F.col("event_type").isin(["purchase", "search", "add_to_cart"]))
    .groupBy(
        F.when(F.col("duration_sec") >= 417, "high").when(F.col("duration_sec") >= 173, "mid").otherwise("low").alias("duration_sec_bucket"),
        "session_id",
    )
    .agg(F.count("*").alias("web_event_count"))
)
