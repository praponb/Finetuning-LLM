from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Car%") & (F.col("credit_limit") <= 10400))
    .select("id", "name", "city", "segment")
)
