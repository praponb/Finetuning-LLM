from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-06-01")
    .groupBy(
        "account_type",
        F.when(F.col("balance") >= 78000, "high").when(F.col("balance") >= 15000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.countDistinct("balance").alias("unique_statuss"))
    .filter(F.col("unique_statuss") < 9)
    .select("account_type", "balance_bucket", "unique_statuss")
    .orderBy(F.col("balance_bucket").desc())
)
