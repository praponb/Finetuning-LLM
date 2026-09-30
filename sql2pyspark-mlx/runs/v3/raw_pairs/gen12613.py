from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 190) | F.col("monthly_fee").between(130, 220))
    .select(
        "start_date",
        "plan",
        F.concat_ws("-", "plan", "status").alias("plan_status_key"),
    )
    .orderBy("plan_status_key", "start_date")
)
