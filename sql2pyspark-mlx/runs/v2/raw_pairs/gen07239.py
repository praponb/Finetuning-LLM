from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() | (F.col("amount") <= 1310))
    .groupBy(F.year("paid_at").alias("paid_year"), "method")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
    )
    .filter(F.col("bank_transfer_amount") < 31)
    .drop("bank_transfer_amount")
    .orderBy("row_count")
)
