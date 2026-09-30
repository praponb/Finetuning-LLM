from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(38, 65))
    .select("credit_limit", "city", "name", F.trim("country").alias("clean_country"))
)
