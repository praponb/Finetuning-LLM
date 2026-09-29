from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("unique_merchants") > 7)
    .select("txn_month", "unique_merchants", "avg_amount")
    .orderBy("txn_month")
)
