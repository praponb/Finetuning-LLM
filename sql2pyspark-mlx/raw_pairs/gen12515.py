from pyspark.sql import functions as F

result = (
    df_transactions
    .select("account_id", "id")
    .exceptAll(
        df_transactions
        .filter(F.col("txn_type") == "DEBIT")
        .select("account_id", "id")
    )
)
