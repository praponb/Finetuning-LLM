from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 6400)
    .select("credit_limit", "is_active", "name")
)
