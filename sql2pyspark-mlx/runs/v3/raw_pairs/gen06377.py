from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.when(F.col("event_type") == "search", 1).otherwise(0).alias("is_search"),
    )
    .orderBy("is_search")
)
