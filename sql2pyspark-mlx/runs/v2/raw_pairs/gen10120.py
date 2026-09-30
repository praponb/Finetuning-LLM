from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.quarter("signup_date").alias("signup_quarter"),
        "is_active",
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.count("age").alias("age_count"),
        F.sum(F.when(F.col("country") == "SG", F.col("credit_limit")).otherwise(0)).alias("sg_credit_limit"),
    )
)
