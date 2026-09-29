from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "FEE", "REFUND"]) & (F.col("txn_type") == "FEE") & (F.to_date("txn_time") >= "2024-11-01"))
    .groupBy("merchant")
    .agg(F.countDistinct("amount").alias("unique_accounts"))
    .filter(F.col("unique_accounts") >= 5)
)
