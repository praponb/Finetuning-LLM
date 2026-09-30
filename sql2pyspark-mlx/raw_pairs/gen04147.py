from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 15) & (F.col("store_id") == 31))
    .select(
        "id",
        "channel",
        "amount",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.greatest("amount", "discount").alias("greatest_amount_discount"),
    )
    .orderBy("channel", "id")
    .limit(5)
)
