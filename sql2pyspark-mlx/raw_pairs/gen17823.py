from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "account_id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("amount").between(1200, 2500))
        .select("id", "account_id", "txn_type")
    )
)
