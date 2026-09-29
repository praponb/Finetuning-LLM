from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@outlook.com"))
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
    )
)
