from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(~F.col("merchant").isin(["Amazon", "Grab"]))
    .withColumn("weeks_since_txn_time", (F.datediff(F.current_date(), "txn_time") / 7))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("OTHER")))
)

result = (
    base
    .groupBy(F.date_trunc("WEEK", "txn_time").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .orderBy("week_start")
)
