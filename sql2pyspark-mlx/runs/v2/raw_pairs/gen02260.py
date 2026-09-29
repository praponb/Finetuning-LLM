from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-11-25")
    .groupBy(
        "account_id",
        F.when(F.col("amount") >= 4500, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 9)
    .select("account_id", "amount_bucket", "cnt")
)
