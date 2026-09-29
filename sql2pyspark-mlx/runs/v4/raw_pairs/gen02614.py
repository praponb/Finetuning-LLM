from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
    .filter(F.col("singapore_count") >= 60)
)
