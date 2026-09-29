from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 7, 14]))
    .select("merchant")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type") == "REFUND")
        .select("merchant")
    )
    .distinct()
)
