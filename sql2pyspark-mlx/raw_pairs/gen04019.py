from pyspark.sql import functions as F

base = df_customers.filter((F.col("signup_date") < "2024-02-09") | (F.col("age") > 32))

customers_stats = (
    base
    .groupBy("segment")
    .agg(F.max("credit_limit").alias("max_credit_limit"))
)

result = customers_stats.filter(F.col("max_credit_limit") > 6100)
