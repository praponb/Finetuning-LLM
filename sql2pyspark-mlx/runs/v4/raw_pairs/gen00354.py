from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1400)
    .select("id", "txn_type", "account_id")
    .unionByName(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("id", "txn_type", "account_id")
    )
    .orderBy(F.col("id").desc())
)
