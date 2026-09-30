from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("signup_date").between("2024-05-13", "2025-01-01"))
    .withColumn("credit_limit_times_age", (F.col("credit_limit") * F.col("age")))
)

agg = (
    base
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"))
)

result = (
    agg
    .filter(F.col("bangkok_count") < 8)
    .select("signup_year", "bangkok_count")
    .orderBy(F.col("bangkok_count").desc(), "signup_year")
    .limit(1)
)
