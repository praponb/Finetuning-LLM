from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "7-Eleven", "Shell"]))
    .groupBy(
        F.when(F.col("txn_type").isin(["CREDIT", "REFUND"]), "group_a").otherwise("other").alias("txn_type_group"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 48)
    .select("txn_type_group", "avg_amount")
)
