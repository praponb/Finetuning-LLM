from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2025-10-01")
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("cancelled_monthly_fee") > 55)
    .select("plan", "cancelled_monthly_fee", "plan_list")
)
