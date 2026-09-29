from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2023-04-17")
    .select("account_id")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type") == "FEE")
        .select("account_id")
    )
)
