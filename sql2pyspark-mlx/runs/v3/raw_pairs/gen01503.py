from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.tablet_count") >= 9)
    .select(F.col("c.id"), F.col("c.name"), F.col("sbc.tablet_count"))
)
