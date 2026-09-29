from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.segment") != "Consumer")
    .select(F.col("c.id"), F.col("c.name"), F.col("c.is_active"), F.col("c.city"))
    .orderBy(F.col("city").desc(), F.col("id").desc())
)
