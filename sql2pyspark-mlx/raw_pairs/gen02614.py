from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "7-Eleven") & (F.col("amount") <= 2800))
    .groupBy("is_flagged")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
    .orderBy(F.col("unique_txn_types").desc())
)
