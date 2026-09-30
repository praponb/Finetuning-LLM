from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(~((F.col("c.country") == "SG") | (F.col("s.started_at") < "2024-01-01")))
    .select(F.col("s.country"), F.col("c.is_active"))
    .orderBy("country")
)
