from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 8)
    .select("is_active", "cnt")
    .orderBy("cnt")
)
