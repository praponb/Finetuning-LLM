from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count("amount").alias("amount_count"),
        F.max("shipping_fee").alias("max_shipping_fee"),
    )
    .filter(F.col("amount_count") >= 4)
    .select("channel", "amount_count", "max_shipping_fee")
)
