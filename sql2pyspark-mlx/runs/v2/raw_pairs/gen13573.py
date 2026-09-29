from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("country") == "DE")

customers_stats = (
    filtered_customers
    .groupBy("is_active")
    .agg(F.min("credit_limit").alias("min_credit_limit"))
)

result = customers_stats
