from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Shell", "Lazada"]))
    .select("id", "merchant", "txn_type")
    .orderBy(F.col("id").desc())
)
