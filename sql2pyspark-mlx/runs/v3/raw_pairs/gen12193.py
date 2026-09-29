from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 39)
    .orderBy(F.col("customer_id").desc())
)
