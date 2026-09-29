from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.segment"))
    .agg(F.count(F.when(F.col("s.country") == "SG", 1)).alias("sg_count"))
)
