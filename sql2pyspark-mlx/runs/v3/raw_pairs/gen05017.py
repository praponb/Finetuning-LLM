from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.count("*").alias("total_count"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .orderBy(F.col("total_count").desc(), "segment")
    .limit(10)
)
