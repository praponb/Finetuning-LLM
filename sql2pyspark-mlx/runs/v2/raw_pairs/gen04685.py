from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "7-Eleven")
    .select("id", "txn_time")
    .orderBy(F.col("id").desc())
    .limit(5)
)
