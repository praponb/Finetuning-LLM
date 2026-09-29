from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.substring("merchant", 1, 4).alias("merchant_prefix"),
    )
)
