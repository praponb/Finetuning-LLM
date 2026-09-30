from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-02-01")
    .select(
        "id",
        "customer_id",
        F.concat_ws("-", "plan", "status").alias("plan_status_key"),
    )
)
