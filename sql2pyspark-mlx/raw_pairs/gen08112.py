from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 14500) & F.col("country").isin(["US", "JP"]))
    .select("id", "credit_limit", "is_active", "age")
)
