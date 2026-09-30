from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([13, 25, 28, 33]))
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
    .filter(F.col("avg_shipping_fee") > 80)
)
