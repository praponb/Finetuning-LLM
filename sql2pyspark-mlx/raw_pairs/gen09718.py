from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("opened_date") <= "2024-08-21") & (F.col("status") == "OPEN")) | F.col("customer_id").isNull())
    .groupBy(
        "account_type",
        "status",
        F.when(F.col("balance") >= 63000, "high").when(F.col("balance") >= 10000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
