from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("end_date") >= 2025) & (F.year("end_date") >= 2023))
    .select(
        "start_date",
        "customer_id",
        "plan",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.concat_ws(",", "plan", "status").alias("plan_status_key"),
    )
    .orderBy("start_date")
)
