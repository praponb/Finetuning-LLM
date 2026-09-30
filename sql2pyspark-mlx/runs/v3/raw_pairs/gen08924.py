from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2024-05-01") & (F.col("txn_type") == "CREDIT"))
    .select("merchant", "amount")
    .orderBy(F.col("merchant").desc())
)
