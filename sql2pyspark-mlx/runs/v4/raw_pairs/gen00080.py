from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 200)
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
    )
    .orderBy("expired_monthly_fee")
)
