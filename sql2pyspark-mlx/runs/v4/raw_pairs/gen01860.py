from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("status") == "CANCELLED") & (F.col("customer_id").isin([3, 26]) | (F.col("status") != "ACTIVE"))))
    .select(
        "id",
        "end_date",
        "status",
        F.substring("status", 1, 2).alias("status_prefix"),
        F.concat_ws("-", "plan", "status").alias("plan_status_key"),
    )
)
