from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 132) & (F.col("page") == "/cart") & F.col("event_type").isin(["search", "purchase", "add_to_cart"]))
    .select(
        "id",
        "page",
        F.upper("page").alias("page_upper"),
        F.lower("page").alias("page_lower"),
    )
    .orderBy("page_upper", F.col("id").desc())
)
