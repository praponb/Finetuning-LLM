from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("city").isin(["Singapore", "New York", "Tokyo"]) & (F.col("email").isNotNull() | (F.col("credit_limit") < 13600)))
)

summary = (
    filtered_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.count("*").alias("total_count"),
    )
)

result = (
    summary
    .filter(F.col("total_count") > 5)
    .orderBy(F.col("us_count").desc())
)
