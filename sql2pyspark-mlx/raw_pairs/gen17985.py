from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2024-09-06") & (F.to_date("txn_time") >= "2023-10-06") & F.col("merchant").isin(["Shell", "Lazada", "Starbucks"]))
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count("*").alias("row_count"),
    )
    .orderBy("txn_month")
)
