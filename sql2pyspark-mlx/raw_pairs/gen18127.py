from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate", "Consumer"]) & F.col("city").isin(["New York", "Berlin"]))
    .select("id", "signup_date")
)
