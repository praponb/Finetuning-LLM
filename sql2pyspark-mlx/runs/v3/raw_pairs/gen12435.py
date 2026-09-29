from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.hour("txn_time").between(8, 15) & (F.col("amount") >= 1900)) | ~F.col("is_flagged"))
    .groupBy("txn_type")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .orderBy(F.col("credit_amount").desc(), F.col("total_amount").desc(), "txn_type")
    .limit(5)
)
