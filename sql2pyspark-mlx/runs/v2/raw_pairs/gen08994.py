from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "FEE"]) | (F.col("txn_time") >= "2024-10-31"))
    .select("id", "amount", "merchant")
    .orderBy(F.col("amount").desc())
)
