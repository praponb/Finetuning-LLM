from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.col("c.age") <= 47) & (F.col("c.country") != "US"))
    .select(F.col("c.email"), F.col("c.name"), F.col("c.segment"), F.col("s.country"))
)
