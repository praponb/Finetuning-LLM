from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 36) & ((F.col("store_id") == 36) | (F.col("amount") >= 720)))
    .select("shipping_fee", "amount")
)
