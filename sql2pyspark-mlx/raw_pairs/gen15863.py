from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .withColumn("amount_int", F.col("amount").cast("int"))
)
