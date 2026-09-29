from pyspark.sql import functions as F

base = df_customers.filter(F.col("name").like("Iv%"))

city_summary = (
    base
    .groupBy("city")
    .agg(F.count("credit_limit").alias("credit_limit_count"))
)

result = city_summary.select("city", "credit_limit_count")
