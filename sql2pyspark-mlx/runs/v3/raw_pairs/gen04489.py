from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("city") == "Tokyo") & (F.col("signup_date") <= "2023-12-01")) | (F.col("age") < 67))
    .select("id", "is_active")
)
