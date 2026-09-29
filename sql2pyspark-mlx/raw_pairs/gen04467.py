from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant", "txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("merchant").isNotNull())
        .select("merchant", "txn_type")
    )
)
