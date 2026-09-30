from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([2, 5, 7]) & ((F.col("status") == "CANCELLED") | (F.col("start_date") > "2025-05-01")))
    .groupBy(
        F.when(F.col("monthly_fee") >= 184, "high").when(F.col("monthly_fee") >= 87, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
