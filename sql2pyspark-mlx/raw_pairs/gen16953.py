from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("first_started_at"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .select(F.col("c.name"), F.col("sbc.first_started_at"))
    .orderBy(F.col("first_started_at").desc(), "name")
)
