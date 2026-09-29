from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([4, 5, 7]))
    .groupBy("status", "plan")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") < 170)
    .select("status", "plan", "status_values", "max_monthly_fee")
)
