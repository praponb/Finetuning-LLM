from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("txn_type") != "REFUND") & F.col("txn_type").isin(["REFUND", "DEBIT"])) | F.col("is_flagged"))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") > 1300)
    .orderBy(F.col("max_amount").desc())
)
