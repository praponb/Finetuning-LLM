from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["UK", "US"]))
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
        F.sum("age").alias("total_age"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
    .orderBy(F.col("sg_count").desc())
)
