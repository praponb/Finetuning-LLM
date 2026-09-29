from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 226, "high").when(F.col("monthly_fee") >= 46, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "customer_id",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("monthly_fee_bucket", "customer_id")
    .limit(5)
)
