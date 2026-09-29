from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter((F.col("amount") >= 1090) & F.col("amount").between(1570, 1590))
    .withColumn("amount_rounded", F.round("amount", 0))
)

payments_stats = (
    valid_payments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    payments_stats
    .filter(F.col("bank_transfer_count") > 7)
    .select("status", "bank_transfer_count")
    .orderBy("status")
)
