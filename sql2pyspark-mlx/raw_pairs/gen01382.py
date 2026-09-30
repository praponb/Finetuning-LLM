from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.age"), F.col("c.name"), F.col("c.is_active"))
    .orderBy("name", F.col("id").desc())
)
