from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "partner", "store"]))
    .groupBy("customer_id")
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 1090)
)
