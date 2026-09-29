from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .orderBy("txn_time")
)
