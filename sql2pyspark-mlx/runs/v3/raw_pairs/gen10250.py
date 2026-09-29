from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([13, 18, 24]))
    .withColumn("amount_rounded", F.round("amount", 2))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)
