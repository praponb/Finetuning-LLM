from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .select("txn_type", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("txn_time") < "2023-04-02")
        .select("txn_type", "account_id")
    )
)
