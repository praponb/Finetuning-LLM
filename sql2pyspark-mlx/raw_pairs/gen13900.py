from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.city"))
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("cnt").desc())
)
