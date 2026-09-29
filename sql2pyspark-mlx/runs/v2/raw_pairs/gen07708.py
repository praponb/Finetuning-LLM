from pyspark.sql import functions as F

valid_customers = df_customers.filter((F.col("country") == "SG") & (F.col("country") != "US"))

summary = (
    valid_customers
    .groupBy(
        F.quarter("signup_date").alias("signup_quarter"),
        F.month("signup_date").alias("signup_month"),
    )
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
)

result = (
    summary
    .orderBy(F.col("signup_quarter").desc(), "signup_month")
    .limit(3)
)
