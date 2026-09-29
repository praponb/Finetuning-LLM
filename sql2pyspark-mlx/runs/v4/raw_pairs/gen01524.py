from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.to_date("started_at") < "2024-10-12").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.length(F.col("c.email")) >= 13)
    .select(F.col("c.id"), F.col("c.age"))
)
