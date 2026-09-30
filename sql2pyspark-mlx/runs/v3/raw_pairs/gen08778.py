from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "merchant",
        "account_id",
        F.dayofmonth("txn_time").alias("txn_time_day"),
        F.coalesce("merchant", F.lit("unassigned")).alias("merchant_filled"),
    )
    .orderBy("account_id")
)
