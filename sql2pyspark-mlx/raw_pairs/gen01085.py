from pyspark.sql import functions as F

result = (
    df_transactions
    .select(F.lit("transaction").alias("record_type"), "amount")
    .union(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select(F.lit("subscription").alias("record_type"), "monthly_fee")
    )
    .distinct()
    .orderBy("record_type")
)
