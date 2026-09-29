from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("country") == "JP")
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("country") == "US", 1)).alias("us_count"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.us_count") >= 8)
    .select(F.col("c.id"), F.col("c.city"), F.col("sbc.us_count"))
    .orderBy("id")
)
