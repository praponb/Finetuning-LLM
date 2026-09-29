from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.min("started_at").alias("first_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .filter(F.col("sbc.th_count") > 7)
    .select(
        F.col("c.country"),
        F.col("c.is_active"),
        F.col("sbc.th_count"),
        F.col("sbc.first_started_at"),
    )
    .orderBy("first_started_at")
)
