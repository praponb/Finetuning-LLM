from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("merchant").isNull())
        .select("id", "txn_type")
    )
)
