from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2025-05-01") & (F.col("amount") < 1100))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)
