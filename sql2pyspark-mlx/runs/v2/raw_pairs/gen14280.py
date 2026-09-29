from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.min("age").alias("min_age"),
    )
)
