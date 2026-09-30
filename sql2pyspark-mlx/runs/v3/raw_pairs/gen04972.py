from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .select(
        "session_id",
        "page",
        "duration_sec",
        F.when(F.col("page") == "/cart", 1).otherwise(0).alias("is_cart"),
    )
    .orderBy(F.col("is_cart").desc())
)
