from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["REFUND", "DEBIT"]))
    .select("account_id", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type") != "FEE")
        .select("account_id", "merchant")
    )
    .orderBy("merchant")
)
