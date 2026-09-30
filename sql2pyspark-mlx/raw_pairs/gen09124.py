from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select("id", "shipping_fee")
    .orderBy(F.col("id").desc())
)
