from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("customer_id") == 19).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.segment"), F.col("c.is_active"), F.col("c.name"))
    .orderBy(F.col("segment").desc())
)
