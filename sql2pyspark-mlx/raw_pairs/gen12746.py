from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "REFUND"]))
    .select("account_id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("merchant").isin(["Starbucks", "Lazada", "Amazon"]))
        .select("account_id", "txn_type")
    )
    .orderBy("account_id")
)
