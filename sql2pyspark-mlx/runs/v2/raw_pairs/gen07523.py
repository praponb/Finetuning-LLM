from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "REFUND", "CREDIT"]))
    .select("merchant", "account_id")
    .unionByName(
        df_transactions
        .filter(F.col("amount") > 1800)
        .select("merchant", "account_id")
    )
    .orderBy(F.col("account_id").desc())
)
