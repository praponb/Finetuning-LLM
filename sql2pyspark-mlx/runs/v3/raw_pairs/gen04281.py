from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "plan", "customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy(F.col("customer_id").desc(), "status", "plan")
    .limit(5)
)
