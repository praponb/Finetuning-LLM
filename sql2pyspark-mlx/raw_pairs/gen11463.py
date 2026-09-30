from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([3, 8]))
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.min("shipping_fee").alias("min_shipping_fee"))
    .filter(F.col("min_shipping_fee") > 28)
)
