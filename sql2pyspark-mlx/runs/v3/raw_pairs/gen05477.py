from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 6900) & ~F.col("is_active") & (F.col("age") < 35))
    .groupBy("is_active", "country")
    .agg(F.max("signup_date").alias("last_signup_date"))
    .orderBy("last_signup_date", "is_active", "country")
    .limit(1)
)
