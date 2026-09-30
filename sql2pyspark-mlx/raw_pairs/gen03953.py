from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/cart", "/search"])
    .agg(F.avg("duration_sec").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "event_type",
        F.col("/cart_amount").alias("cart_amount"),
        F.col("/cart_n").alias("cart_n"),
        F.col("/search_amount").alias("search_amount"),
        F.col("/search_n").alias("search_n"),
    )
)
