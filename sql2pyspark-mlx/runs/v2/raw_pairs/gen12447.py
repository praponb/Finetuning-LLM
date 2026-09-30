from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(F.col("s.country"), F.col("c.is_active"), F.col("c.name"))
    .orderBy("name")
)
