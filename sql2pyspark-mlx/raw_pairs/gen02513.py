from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2024-06-01")
    .select("merchant", "txn_type")
    .unionByName(
        df_transactions
        .filter(~F.col("txn_type").isin(["REFUND", "FEE"]))
        .select("merchant", "txn_type")
    )
)
