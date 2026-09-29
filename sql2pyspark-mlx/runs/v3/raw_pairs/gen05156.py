from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") < "2023-08-11") & F.col("email").like("%@outlook.com"))
    .groupBy("city")
    .agg(
        F.sum("age").alias("total_age"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .filter(F.col("total_age") > 180)
)
