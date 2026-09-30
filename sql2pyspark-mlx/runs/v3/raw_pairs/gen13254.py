from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["US", "JP", "TH"]) | (F.year("signup_date") == 2025))
    .select("credit_limit", "email", "name")
)
