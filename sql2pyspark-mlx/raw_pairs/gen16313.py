from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant", "txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("account_id").isin([1, 38]))
        .select("merchant", "txn_type")
    )
)
