from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.age") >= 57)
    .select(F.col("c.id"), F.col("c.is_active"))
    .orderBy("id")
    .limit(1)
)
