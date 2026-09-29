from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-08-05")
    .select("merchant", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type").isin(["CREDIT", "REFUND"]))
        .select("merchant", "txn_type")
    )
    .distinct()
)
