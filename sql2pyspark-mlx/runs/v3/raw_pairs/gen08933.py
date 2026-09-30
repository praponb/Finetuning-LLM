from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") >= 36) & (F.col("shipping_fee") <= 37))
    .orderBy(F.col("customer_id").desc())
)
