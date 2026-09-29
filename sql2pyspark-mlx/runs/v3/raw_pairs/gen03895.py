from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("browser") == "Safari").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.segment"), F.col("c.country"))
    .orderBy("country", F.col("id").desc())
)
