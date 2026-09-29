from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2024-11-01") | F.hour("txn_time").between(7, 11))
    .groupBy(
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
