from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(~F.col("city").isin(["Chiang Mai", "New York"])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.hour(F.col("s.started_at")).between(8, 11))
    .select(F.col("s.id"), F.col("s.customer_id"))
)
