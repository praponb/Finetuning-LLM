from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "is_flagged",
        "amount",
        F.hour("txn_time").alias("txn_time_hour"),
        F.date_trunc("MONTH", "txn_time").alias("txn_time_period"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(20)
)
