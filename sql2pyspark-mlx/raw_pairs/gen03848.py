from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("amount_tier", F.when(F.col("amount") >= 3300, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "txn_time"))
    .groupingSets(
        [["amount_tier", "week_start"], ["amount_tier"], []],
        "amount_tier",
        "week_start",
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
