from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(41, 65))
    .groupBy("segment")
    .agg(
        F.min("age").alias("min_age"),
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
    )
    .orderBy("sg_count")
)
