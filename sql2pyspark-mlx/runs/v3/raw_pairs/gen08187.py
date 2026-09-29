from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & ~F.col("account_type").isin(["CREDIT", "CHECKING"]))
    .groupBy(
        F.when(F.col("balance") >= 63000, "high").when(F.col("balance") >= 14000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
    )
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)
