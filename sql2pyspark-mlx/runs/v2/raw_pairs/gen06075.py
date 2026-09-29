from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.count("*").alias("num_customers"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .filter(F.col("de_count") > 4)
    .orderBy(F.col("de_count").desc(), F.col("chiang_mai_count").desc())
)
