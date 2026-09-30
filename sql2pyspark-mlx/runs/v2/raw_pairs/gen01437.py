from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2023-09-01")
    .select("txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([5, 17, 29]))
        .select("txn_type")
    )
    .distinct()
)
