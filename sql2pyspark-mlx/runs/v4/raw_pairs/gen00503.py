from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "REFUND") | F.col("account_id").isin([13, 35]))
    .groupBy("txn_type")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
    .filter(F.col("unique_txn_types") > 3)
    .orderBy("txn_type")
    .limit(3)
)
