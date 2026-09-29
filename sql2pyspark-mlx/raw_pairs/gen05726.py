from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id").isin([1, 25]) & ~F.col("status").isin(["PENDING", "CANCELLED"])) | (F.col("discount") <= 0.14))
    .select("id", "shipping_fee", F.substring("status", 1, 4).alias("status_prefix"))
)
