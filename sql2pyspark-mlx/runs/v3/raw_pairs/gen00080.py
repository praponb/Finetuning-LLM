from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
    .filter(F.col("unique_txn_types") < 2)
)
