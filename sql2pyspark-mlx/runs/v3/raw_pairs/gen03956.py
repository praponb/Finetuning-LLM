from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .select("id", "amount")
    .orderBy("id")
)
