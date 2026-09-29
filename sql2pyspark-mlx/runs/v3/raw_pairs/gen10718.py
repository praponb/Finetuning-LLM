from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .withColumn("txn_time_label", F.date_format("txn_time", "yyyy-MM-dd"))
)
