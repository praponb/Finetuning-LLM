from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("browser").isin(["Chrome", "Safari"])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.country") == "JP")
    .select(F.col("c.id"), F.col("c.segment"), F.col("c.name"), F.col("c.is_active"))
    .orderBy("id")
)
