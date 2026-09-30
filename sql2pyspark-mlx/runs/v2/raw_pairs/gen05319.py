from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "txn_type", F.col("is_flagged").cast("int").alias("is_flagged_flag"))
    .orderBy(F.col("is_flagged_flag").desc())
)
