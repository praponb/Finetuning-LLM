from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "plan",
        F.concat_ws("-", "status", "plan").alias("status_plan_key"),
    )
)
