from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(8000, 8300))
    .select("email")
)
