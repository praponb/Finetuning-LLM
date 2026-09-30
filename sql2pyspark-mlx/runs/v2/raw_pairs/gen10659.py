from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([21, 35]) & (F.to_date("txn_time") < "2024-07-01"))
    .groupBy(
        "txn_type",
        "is_flagged",
        F.when(F.col("amount") >= 4300, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("*").alias("num_transactions"))
    .orderBy("amount_bucket")
)
