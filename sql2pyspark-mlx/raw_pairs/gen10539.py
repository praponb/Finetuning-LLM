from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .filter(F.col("discount_range") >= 75)
    .select("status", "discount_range", "total_shipping_fee")
    .orderBy("total_shipping_fee")
)
