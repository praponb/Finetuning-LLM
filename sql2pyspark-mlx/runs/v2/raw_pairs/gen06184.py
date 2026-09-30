from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "REFUND") & (F.col("txn_type") == "FEE"))
    .groupBy("is_flagged")
    .agg(F.min("amount").alias("min_amount"))
)
