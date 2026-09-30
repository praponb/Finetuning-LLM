from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") >= 1310) & (F.col("shipping_fee") < 26) & (F.col("discount") >= 0.18))
    .select("store_id", "customer_id", "status")
)
