from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", "segment")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.sum(F.when(F.col("city") == "London", F.col("age")).otherwise(0)).alias("london_age"),
    )
    .filter(F.col("london_age") >= 78)
)
