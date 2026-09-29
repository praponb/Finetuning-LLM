from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") >= 9900) & F.col("credit_limit").isNull())
    .select("id", "email", "city", "credit_limit")
)
