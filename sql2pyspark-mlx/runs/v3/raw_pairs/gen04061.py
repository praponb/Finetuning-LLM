from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 1400) | (F.col("amount") > 3700))
    .select("is_flagged", F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"))
    .orderBy("txn_time_label")
)
