from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 4700) & (F.col("city") == "Singapore"))
    .select("age", "signup_date", "name")
)
