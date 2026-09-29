from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("amount") >= 1700) & (F.to_date("txn_time") < "2024-10-01")) | F.col("account_id").isin([29, 31, 33, 39]))
    .select(
        "id",
        "merchant",
        "is_flagged",
        "txn_time",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)
