from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(~F.col("browser").isin(["Firefox", "Chrome", "Safari"]))
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("min_started_at"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(F.col("c.email"), F.col("c.name"), F.col("ct.min_started_at"))
)
