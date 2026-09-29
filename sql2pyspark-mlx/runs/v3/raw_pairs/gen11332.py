from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "DEBIT", "FEE"]))
    .select("id", "merchant")
    .unionByName(
        df_transactions
        .filter(~F.col("merchant").isin(["Lazada", "Amazon", "Grab"]))
        .select("id", "merchant")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
