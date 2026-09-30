from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull() & (F.col("merchant").isin(["7-Eleven", "Amazon"]) | (F.col("txn_type") == "FEE")))
    .select("id", "is_flagged", "amount")
    .orderBy("amount", "id")
    .limit(10)
)
