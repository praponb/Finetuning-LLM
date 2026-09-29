from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/product", "/checkout"]) | (F.col("event_time") < "2023-11-26"))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.when(F.col("event_type") == "search", 1).otherwise(0).alias("is_search"),
    )
)
