from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.col("is_active"))
    .select("id", "segment", "credit_limit", "signup_date")
    .orderBy(F.col("credit_limit").desc())
)
