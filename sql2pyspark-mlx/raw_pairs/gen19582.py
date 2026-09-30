from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 13)
    .select("id", "shipping_fee", "store_id", "channel")
)
