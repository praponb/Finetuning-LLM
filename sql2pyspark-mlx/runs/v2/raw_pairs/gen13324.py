from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/home"]))
    .select(
        "id",
        "page",
        "event_type",
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
)
