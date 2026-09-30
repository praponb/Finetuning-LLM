from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id").isin([2, 11, 18, 37]) & (F.col("merchant") == "Amazon")) | (F.col("merchant") == "Amazon"))
    .groupBy("txn_type")
    .agg(F.countDistinct("amount").alias("unique_accounts"))
)
