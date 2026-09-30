from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
    .filter(F.col("unique_merchants") < 1)
    .drop("unique_merchants")
    .orderBy(F.col("total_amount").desc())
)
