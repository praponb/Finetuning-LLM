from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").desc_nulls_last())

latest = (
    df_transactions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .rollup("merchant", "activity_month", "txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.grouping("merchant").alias("grouping_level"),
    )
    .orderBy(F.col("amazon_amount").desc_nulls_last())
)
