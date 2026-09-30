from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "monthly_fee",
        "plan",
        "status",
        F.when(F.col("status").isin(["ACTIVE", "CANCELLED"]), "Priority").otherwise("Other").alias("status_group"),
    )
    .orderBy("plan")
)
