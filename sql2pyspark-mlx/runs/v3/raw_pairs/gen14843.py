from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 78000, "high").when(F.col("balance") >= 34000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
    )
    .agg(F.countDistinct("balance").alias("unique_statuss"))
)
