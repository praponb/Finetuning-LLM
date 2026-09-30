from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "enterprise", "standard"]) & ((F.col("status") == "CANCELLED") | F.col("plan").isin(["standard", "premium"])))
    .groupBy("status")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 2)
    .drop("unique_statuss")
    .orderBy("status")
)
