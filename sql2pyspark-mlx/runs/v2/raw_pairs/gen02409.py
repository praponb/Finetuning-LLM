from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .filter(F.col("avg_amount") >= 1100)
    .select("txn_day", "avg_amount", "txn_type_list")
)
