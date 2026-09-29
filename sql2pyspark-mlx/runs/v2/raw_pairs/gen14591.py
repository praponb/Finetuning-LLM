from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([10, 19, 28]))
    .select("account_id", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("txn_time") < "2023-06-25")
        .select("account_id", "txn_type")
    )
    .distinct()
)
