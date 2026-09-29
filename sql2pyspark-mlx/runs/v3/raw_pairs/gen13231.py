from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 38)
    .select(
        "id",
        "channel",
        "discount",
        F.ceil("discount").alias("discount_whole"),
        F.when(F.col("amount") >= 1310, "High").when(F.col("amount") >= 640, "Medium").otherwise("Low").alias("amount_tier"),
    )
    .orderBy(F.col("discount").desc(), F.col("amount_tier").desc())
)
