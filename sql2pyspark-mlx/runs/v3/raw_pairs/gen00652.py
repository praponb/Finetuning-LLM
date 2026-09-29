from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB", "Consumer"]) & (F.col("country") == "DE"))
    .select("id", "credit_limit")
)
