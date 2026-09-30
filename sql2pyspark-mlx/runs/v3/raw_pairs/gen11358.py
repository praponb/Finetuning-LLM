from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") <= 0.26)
    .select("store_id", "shipping_fee")
    .orderBy(F.col("shipping_fee").desc())
)
