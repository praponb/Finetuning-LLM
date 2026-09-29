from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2023-12-23")
    .select("id", "account_id", "merchant")
    .subtract(
        df_transactions
        .filter(F.to_date("txn_time") >= "2025-03-02")
        .select("id", "account_id", "merchant")
    )
    .orderBy("id")
)
