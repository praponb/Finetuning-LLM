from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNotNull() & F.col("credit_limit").between(6500, 10800))
    .select("id", "segment")
    .orderBy("segment")
)
