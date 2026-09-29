from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.max("discount").alias("max_discount"))
    .orderBy("status", F.col("max_discount").desc())
    .limit(3)
)
