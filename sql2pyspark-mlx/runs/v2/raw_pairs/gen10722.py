from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") > 3200) & (~F.col("is_flagged") | F.col("is_flagged")))
    .select(
        "id",
        "account_id",
        "txn_time",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.when(F.col("txn_type") == "FEE", 1).otherwise(0).alias("is_fee"),
    )
    .orderBy(F.col("amount_filled").desc())
)
