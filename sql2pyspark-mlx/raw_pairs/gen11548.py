from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 41000)
    .groupBy(
        F.when(F.col("balance") >= 58000, "high").when(F.col("balance") >= 35000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)
