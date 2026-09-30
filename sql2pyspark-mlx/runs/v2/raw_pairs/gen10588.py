from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "amount",
        "account_id",
        "merchant",
        F.floor("amount").alias("amount_whole"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("account_id").desc())
)
