from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNotNull() & F.col("email").like("%@company.co") & F.col("email").isNull())
    .cube(F.month("signup_date").alias("signup_month"), "segment")
    .agg(F.max("signup_date").alias("latest_signup_date"))
)
