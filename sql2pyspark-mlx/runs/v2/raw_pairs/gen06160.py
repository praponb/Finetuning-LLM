from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
)
