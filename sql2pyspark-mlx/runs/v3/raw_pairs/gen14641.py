from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "enterprise", "basic"]))
    .groupBy("customer_id")
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("C.latest_end_date"),
        F.col("C.enterprise_count"),
    )
    .orderBy("name")
)
