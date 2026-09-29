from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 2, 10]))
    .select("id", "txn_type")
    .intersect(
        df_transactions
        .filter(F.col("merchant").isin(["7-Eleven", "Lazada"]))
        .select("id", "txn_type")
    )
    .orderBy("txn_type")
)
