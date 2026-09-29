from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4000, "HIGH").otherwise("LOW").alias("amount_tier"),
        "account_id",
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("merchant") == "Grab").alias("grab_events"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
    )
)
