from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") > 0.18) & F.col("store_id").isNotNull() & F.col("store_id").isNotNull())
    .groupBy("customer_id", "channel")
    .agg(F.min("discount").alias("min_discount"))
    .orderBy(F.col("min_discount").desc())
)
