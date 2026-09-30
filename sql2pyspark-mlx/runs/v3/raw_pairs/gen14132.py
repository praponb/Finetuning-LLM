from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 12)
    .select("id", "shipping_fee", "customer_id", "channel")
)
