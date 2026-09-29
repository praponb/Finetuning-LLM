from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 212, "high").when(F.col("monthly_fee") >= 36, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.max("start_date").alias("last_start_date"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy("last_start_date", "monthly_fee_bucket")
)
