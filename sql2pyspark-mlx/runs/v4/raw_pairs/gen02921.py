from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy(
        F.when(F.col("txn_type").isin(["REFUND", "FEE"]), "core").otherwise("other").alias("txn_type_group"),
    )
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy("amount_range")
)
