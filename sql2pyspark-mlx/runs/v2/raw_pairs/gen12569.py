from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Lazada", "Shell"]))
    .select("id", "txn_time")
    .orderBy(F.col("txn_time").desc())
)
