from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("customer_id") == 34)
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("device") == "mobile", 1).otherwise(0)).alias("mobile_count"),
        F.min("started_at").alias("first_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("S"), F.col("c.id") == F.col("S.customer_id"), "inner")
    .filter(F.col("S.mobile_count") > 61)
    .select(F.col("c.segment"), F.col("S.mobile_count"), F.col("S.first_started_at"))
)
