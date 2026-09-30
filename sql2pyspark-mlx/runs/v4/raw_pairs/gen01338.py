from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 14) | F.col("is_flagged"))
    .select(
        "id",
        "merchant",
        "account_id",
        "txn_time",
        F.length("merchant").alias("merchant_length"),
    )
    .orderBy(F.col("merchant_length").desc())
)
