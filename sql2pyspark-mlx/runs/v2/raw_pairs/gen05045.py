from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(10, 14))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)
