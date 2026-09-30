from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Lazada", "Starbucks", "Grab"]))
    .select("account_id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.hour("txn_time").between(12, 15))
        .select("account_id", "txn_type")
    )
    .orderBy("account_id")
)
