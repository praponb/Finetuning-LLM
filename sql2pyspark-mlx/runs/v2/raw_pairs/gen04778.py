from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~(F.hour("txn_time").between(10, 16) & F.col("account_id").isin([1, 7, 19])))
    .groupBy(
        "txn_type",
        F.when(F.col("amount") >= 3500, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"))
    .orderBy("amount_bucket")
)
