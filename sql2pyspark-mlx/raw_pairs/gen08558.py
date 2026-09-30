from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        F.concat_ws(",", "plan", "status").alias("plan_status_key"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
