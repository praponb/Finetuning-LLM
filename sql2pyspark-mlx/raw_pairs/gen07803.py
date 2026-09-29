from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 16))
    .select("id", "txn_type", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("merchant").isin(["Starbucks", "Amazon"]))
        .select("id", "txn_type", "account_id")
    )
    .orderBy(F.col("id").desc())
)
