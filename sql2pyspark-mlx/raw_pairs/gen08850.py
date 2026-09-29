from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & F.col("shipping_fee").isNotNull())
    .select("id", "status", "discount", "store_id")
    .orderBy("id", F.col("status").desc())
)
