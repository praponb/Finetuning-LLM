from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "start_date",
        "status",
        "end_date",
        F.concat_ws("-", "status", "plan").alias("status_plan_key"),
    )
)
