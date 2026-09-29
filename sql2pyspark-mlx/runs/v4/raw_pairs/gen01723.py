from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([3, 10]) & (F.col("store_id").isNotNull() | (F.col("order_date") > "2025-07-29")))
    .select("shipping_fee")
    .orderBy(F.col("shipping_fee").desc())
)
