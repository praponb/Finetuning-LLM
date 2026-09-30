from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(11, 15))
    .groupBy(
        F.when(F.col("amount") >= 3200, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
        F.to_date("txn_time").alias("txn_day"),
        "txn_type",
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("amount_bucket", "avg_amount")
)
