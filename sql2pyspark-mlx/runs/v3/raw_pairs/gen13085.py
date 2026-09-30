from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["DE", "JP", "UK"]) & F.col("country").isin(["UK", "SG", "JP"]))
    .select("credit_limit")
)
