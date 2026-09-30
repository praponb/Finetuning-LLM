from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg((F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"))
)
