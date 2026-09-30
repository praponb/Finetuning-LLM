from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") >= 10100) | F.col("country").isin(["TH", "UK"]))
)
