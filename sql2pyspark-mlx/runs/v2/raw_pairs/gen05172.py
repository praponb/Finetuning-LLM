from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan", "status", "customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
    )
    .filter(F.col("expired_count") >= 5)
    .drop("expired_count")
    .orderBy("plan_values")
)
