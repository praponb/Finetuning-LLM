from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.count("age").alias("age_count"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
)
