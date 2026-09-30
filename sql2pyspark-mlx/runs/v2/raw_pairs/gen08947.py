from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 40)
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)
