from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.when(F.col("plan").isin(["enterprise", "basic"]), "Group A").otherwise("Other").alias("plan_group"),
    )
)
