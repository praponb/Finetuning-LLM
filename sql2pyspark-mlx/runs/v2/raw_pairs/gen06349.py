from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("status").isin(["ACTIVE", "PAUSED"]))
    .select(
        "id",
        "end_date",
        "status",
        "customer_id",
        F.concat("status", F.lit(" "), "plan").alias("status_plan"),
    )
)
