from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "event_time",
        "event_type",
        F.when(F.col("page") == "/search", 1).otherwise(0).alias("is_search"),
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
    .orderBy(F.col("is_purchase").desc(), F.col("id").desc())
)
