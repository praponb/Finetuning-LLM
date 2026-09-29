from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("segment") != "Consumer") & F.col("signup_date").between("2024-09-01", "2025-10-01"))
)

city_summary = (
    recent_customers
    .groupBy("city")
    .agg(F.count("*").alias("num_customers"))
)

result = (
    city_summary
    .filter(F.col("num_customers") < 5)
    .orderBy("num_customers", "city")
    .limit(3)
)
