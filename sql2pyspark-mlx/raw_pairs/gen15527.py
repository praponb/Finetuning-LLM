from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([14, 29]))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("years_since_txn_time", (F.datediff(F.current_date(), "txn_time") / 365))
    .orderBy("is_flagged_flag", "merchant")
)
