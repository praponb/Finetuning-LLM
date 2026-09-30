from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 9))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

txn_day_summary = (
    recent_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.min("amount").alias("min_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    txn_day_summary
    .filter(F.col("avg_amount") < 3200)
    .select("txn_day", "min_amount", "avg_amount")
    .orderBy("avg_amount", "min_amount")
)
