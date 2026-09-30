from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1500, 2100))
    .select("id", "account_id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("merchant") == "7-Eleven")
        .select("id", "account_id", "txn_type")
    )
)
