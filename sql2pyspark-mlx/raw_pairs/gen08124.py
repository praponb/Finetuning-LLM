from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2023-09-01")
    .select("account_id", "merchant")
    .intersect(
        df_transactions
        .filter(F.col("txn_type").isin(["DEBIT", "CREDIT", "FEE"]))
        .select("account_id", "merchant")
    )
)
