from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.max("start_date").alias("last_start_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.age"), F.col("c.country"), F.col("cs.last_start_date"))
    .orderBy("country")
)
