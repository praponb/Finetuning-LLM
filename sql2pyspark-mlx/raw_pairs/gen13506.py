from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("merchant", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("account_id").isin([22, 27, 28, 37]))
        .select("merchant", "txn_type")
    )
    .orderBy("merchant")
)
