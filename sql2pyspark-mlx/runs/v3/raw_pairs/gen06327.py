from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 7400) & ((F.col("age") < 45) | (F.col("signup_date") < "2024-11-08")))
    .select("age", "city", "country")
)
