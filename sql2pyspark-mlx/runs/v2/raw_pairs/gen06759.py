from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city") == "Bangkok")
    .groupBy(F.col("c.city"))
    .agg(F.count(F.col("s.id")).alias("session_count"))
    .filter(F.col("session_count") >= 3)
    .orderBy("city")
)
