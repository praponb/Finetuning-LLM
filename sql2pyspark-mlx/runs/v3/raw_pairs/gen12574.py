from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 10, 30]) & ~F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))
    .withColumn("amount_int", F.col("amount").cast("int"))
)
