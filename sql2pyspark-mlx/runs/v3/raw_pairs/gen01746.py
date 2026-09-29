from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() | (F.col("start_date") <= "2025-07-01"))
    .withColumn("status_lower", F.lower("status"))
)

agg = (
    recent_subscriptions
    .groupBy("customer_id", "plan")
    .agg(
        F.min("end_date").alias("min_end_date"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)

result = agg.orderBy(F.col("customer_id").desc(), F.col("plan").desc())
