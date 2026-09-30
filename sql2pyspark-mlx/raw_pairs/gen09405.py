from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type")
    .intersect(
        df_subscriptions
        .filter(F.col("status") == "ACTIVE")
        .select(F.col("plan").alias("txn_type"))
    )
)
