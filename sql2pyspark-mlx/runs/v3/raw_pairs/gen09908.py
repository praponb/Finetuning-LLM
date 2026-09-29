from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Shell")
    .select("id", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("merchant").isin(["Amazon", "Starbucks"]))
        .select("id", "txn_type")
    )
    .distinct()
)
