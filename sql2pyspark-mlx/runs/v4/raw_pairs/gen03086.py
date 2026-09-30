from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "DEBIT"]) | F.hour("txn_time").between(10, 16))
    .withColumn("txn_time_date", F.to_date("txn_time"))
    .withColumn("amount_whole", F.ceil("amount"))
)
