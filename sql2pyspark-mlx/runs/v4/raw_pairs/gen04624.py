from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 2700) & ~F.col("merchant").isin(["Lazada", "7-Eleven", "Amazon"]))
    .groupBy("account_id", "is_flagged")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
    .filter(F.col("debit_amount") < 49)
    .drop("debit_amount")
    .orderBy(F.col("account_id").desc())
)
