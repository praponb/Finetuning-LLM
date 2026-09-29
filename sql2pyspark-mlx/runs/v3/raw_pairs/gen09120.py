from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") < 1050) & (F.col("method").isNotNull() | (F.col("order_id") == 21)))
    .groupBy(F.year("paid_at").alias("paid_year"), "method")
    .agg(
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        F.countDistinct("amount").alias("unique_methods"),
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
    )
    .filter(F.col("bank_transfer_count") < 4)
    .drop("bank_transfer_count")
    .orderBy(F.col("method").desc(), "paid_year")
    .limit(20)
)
