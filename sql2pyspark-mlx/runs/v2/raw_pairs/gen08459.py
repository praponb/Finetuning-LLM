from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 3, 25]))
    .select("txn_type", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("txn_type", "account_id")
    )
)
