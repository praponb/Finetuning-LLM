from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") <= "2024-06-01") & (F.col("age") <= 58) & F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
    .select("id", "country", "credit_limit")
)
