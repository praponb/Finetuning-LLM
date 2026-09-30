from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") == 59) & (F.col("credit_limit") <= 8900))
    .select("id", "email", "segment", F.lower("name").alias("name_lower"))
    .orderBy("id", "email")
)
