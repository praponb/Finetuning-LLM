from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "is_flagged",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)
