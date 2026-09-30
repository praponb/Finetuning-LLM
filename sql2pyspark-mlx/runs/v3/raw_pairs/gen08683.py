from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.standard_count") >= 73)
    .select(
        F.col("c.city"),
        F.col("c.age"),
        F.col("cs.latest_end_date"),
        F.col("cs.standard_count"),
    )
    .orderBy("latest_end_date")
)
