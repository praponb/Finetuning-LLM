from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "DEBIT"]))
    .select("id", "merchant")
    .intersect(
        df_transactions
        .filter(F.col("merchant") != "Amazon")
        .select("id", "merchant")
    )
)
