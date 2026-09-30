from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.countDistinct("credit_limit").alias("unique_segments"),
        F.count("age").alias("age_count"),
    )
)
