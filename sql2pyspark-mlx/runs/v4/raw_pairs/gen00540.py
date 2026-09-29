from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.col("country") == "TH") & (F.col("credit_limit") <= 14000) & F.col("signup_date").between("2024-08-01", "2025-07-01")))
    .select("id", "country", "is_active", F.length("city").alias("city_length"))
    .orderBy(F.col("id").desc())
)
