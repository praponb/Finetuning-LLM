from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("record_type"), "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan").isin(["enterprise", "standard", "basic"]))
        .select(F.lit("subscription").alias("record_type"), "status")
    )
    .distinct()
)
