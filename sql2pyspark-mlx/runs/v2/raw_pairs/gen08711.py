from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", "segment")
    .agg(F.min("signup_date").alias("first_signup_date"))
    .orderBy(F.col("segment").desc(), "first_signup_date")
)
