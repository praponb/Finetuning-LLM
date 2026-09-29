from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.country") != "SG")
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(F.count(F.col("s.id")).alias("session_count"))
    .orderBy(F.col("id").desc(), "segment")
)
