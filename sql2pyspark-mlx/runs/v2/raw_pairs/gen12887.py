from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE"]))
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
    .filter(F.col("cancelled_monthly_fee") < 77)
)
