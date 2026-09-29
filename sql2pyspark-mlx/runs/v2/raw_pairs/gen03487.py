from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard", "premium"]) & (F.col("customer_id") == 19))
    .select(
        "id",
        "plan",
        "status",
        F.upper("status").alias("status_upper"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)
