from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "REFUND")
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("amount_whole", F.floor("amount"))
    .orderBy(F.col("amount_whole").desc(), "amount", "id")
    .limit(50)
)
