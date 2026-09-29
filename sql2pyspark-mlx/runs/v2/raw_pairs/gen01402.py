from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 70) & (F.col("plan") == "basic"))
    .select(
        "id",
        "start_date",
        "plan",
        "monthly_fee",
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
        F.concat_ws("/", "plan", "status").alias("plan_status_key"),
    )
)
