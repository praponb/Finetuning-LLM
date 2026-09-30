from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(F.col("c.age"), F.col("ct.latest_end_date"), F.col("ct.plan_list"))
    .orderBy("latest_end_date")
)
