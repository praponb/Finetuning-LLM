from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("shipping_fee") <= 13) & (F.col("customer_id") == 20)) | (F.col("amount") > 1410))
    .agg(F.min("order_date").alias("min_order_date"))
)
