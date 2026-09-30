from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .agg(
        F.count("age").alias("age_count"),
        F.count("*").alias("total_count"),
    )
)
