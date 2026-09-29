from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.max("start_date").alias("latest_start_date"))
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("customer_totals"), F.col("customers.id") == F.col("customer_totals.customer_id"), "left")
    .select(F.col("customers.is_active"), F.col("customer_totals.latest_start_date"))
    .orderBy(F.col("latest_start_date").desc())
)
