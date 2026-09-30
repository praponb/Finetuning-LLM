from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([13, 21, 38]))
    .select("merchant", "txn_type")
    .intersect(
        df_transactions
        .filter(F.col("amount") >= 2200)
        .select("merchant", "txn_type")
    )
)
