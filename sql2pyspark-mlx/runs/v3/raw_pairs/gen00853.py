from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("shipping_fee")).otherwise(0)).alias("returned_shipping_fee"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("returned_shipping_fee").desc())
)
