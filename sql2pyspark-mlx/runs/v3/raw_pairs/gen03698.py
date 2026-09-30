from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-10-26")
    .select("merchant")
    .orderBy(F.col("merchant").desc())
)
