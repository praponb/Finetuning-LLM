from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(18, 35) | F.col("customer_id").isin([1, 7, 8]))
    .select("shipping_fee", "discount", F.lower("status").alias("status_lower"))
)
