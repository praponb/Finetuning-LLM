from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "Tokyo")
    .groupBy(F.month("signup_date").alias("signup_month"), "city")
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
    )
    .orderBy(F.col("city").desc(), F.col("signup_month").desc())
)
