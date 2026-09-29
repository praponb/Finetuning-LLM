from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "is_flagged",
        "txn_time",
        "amount",
        F.substring("txn_type", 1, 4).alias("txn_type_prefix"),
    )
)
