from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("s.browser") != "Chrome")
    .select(F.col("s.browser"), F.col("c.name"), F.col("c.age"))
)
