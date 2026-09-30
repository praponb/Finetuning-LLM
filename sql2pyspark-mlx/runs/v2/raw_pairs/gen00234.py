from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("first_started_at"))
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .select(F.col("customers.is_active"), F.col("t.first_started_at"))
    .orderBy("first_started_at")
)
