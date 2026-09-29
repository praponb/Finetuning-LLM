from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-05-01", "2023-05-03") & (F.col("city") == "Tokyo"))
    .groupBy("country", "is_active")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
)
