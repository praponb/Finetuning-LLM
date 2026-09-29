from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") > 1300) | (F.col("merchant") == "Starbucks"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("txn_time_dayofweek", F.dayofweek("txn_time"))
)
