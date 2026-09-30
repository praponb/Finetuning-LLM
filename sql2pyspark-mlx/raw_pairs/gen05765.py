from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 15500) & ((F.col("city") != "Tokyo") | F.col("is_active")))
    .select("credit_limit")
)
