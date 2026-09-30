from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") > 63) & F.col("signup_date").between("2024-04-01", "2025-09-01"))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.count("*").alias("num_customers"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("num_customers") > 5)
    .orderBy(F.col("total_count").desc())
)
