from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNull())
    .select("id", "account_id")
    .intersect(
        df_transactions
        .filter(~F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))
        .select("id", "account_id")
    )
)
