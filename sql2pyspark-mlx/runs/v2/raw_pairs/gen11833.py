from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") < 7400) & (F.col("age") != 38))
    .select("id", "email", "city", F.lower("email").alias("email_lower"))
)
