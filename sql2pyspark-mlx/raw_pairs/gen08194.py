from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-08-01")
    .select("id", "account_id", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "account_id", "txn_type")
    )
    .distinct()
)
