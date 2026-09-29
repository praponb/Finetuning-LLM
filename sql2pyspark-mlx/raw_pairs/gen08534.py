from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("status") != "CLOSED") | F.col("account_type").isin(["CHECKING", "LOAN"]))
)

balance_bucket_summary = (
    valid_accounts
    .groupBy(
        F.when(F.col("balance") >= 83000, "high").when(F.col("balance") >= 21000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)

result = (
    balance_bucket_summary
    .select("balance_bucket", "stddev_balance")
    .orderBy("balance_bucket")
    .limit(5)
)
