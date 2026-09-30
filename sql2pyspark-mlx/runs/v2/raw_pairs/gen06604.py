from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.length(F.col("c.email")) > 7)
    .select(F.col("c.id"), F.col("c.country"), F.col("c.segment"), F.col("c.email"))
)
