from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "is_flagged",
        "account_id",
        F.concat("txn_type", F.lit(" - "), "merchant").alias("txn_type_merchant"),
    )
)
