from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("is_credit", F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)
