from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & F.col("country").isin(["SG", "DE"]))
    .groupBy("segment", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
)
