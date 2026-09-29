from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
        F.max("signup_date").alias("latest_signup_date"),
        F.countDistinct("credit_limit").alias("unique_segments"),
    )
)
