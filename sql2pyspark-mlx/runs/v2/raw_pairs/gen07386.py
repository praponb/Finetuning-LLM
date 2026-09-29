from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .groupBy("channel")
    .agg(F.max("discount").alias("max_discount"))
    .filter(F.col("max_discount") > 0.34)
    .orderBy(F.col("max_discount").desc())
)
