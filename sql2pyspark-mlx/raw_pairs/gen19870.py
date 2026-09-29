from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") >= 10300) | F.col("age").between(35, 52))
    .select("id", "email", "city", "country")
)
