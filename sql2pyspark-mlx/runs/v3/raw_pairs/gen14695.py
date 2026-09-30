from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2024-05-01") & (F.col("page") == "/cart"))
    .select(
        "id",
        "session_id",
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
