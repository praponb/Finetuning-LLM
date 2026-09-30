from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.max("end_date").alias("latest_end_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.credit_limit"), F.col("c.segment"), F.col("C.latest_end_date"))
    .orderBy("credit_limit", F.col("latest_end_date").desc())
)
