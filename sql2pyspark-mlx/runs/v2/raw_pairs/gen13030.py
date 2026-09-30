from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") < 15500) & (F.col("credit_limit") < 5100))
    .select("country", "age")
)
