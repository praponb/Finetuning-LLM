from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 60000, "high").when(F.col("balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") > 47)
    .orderBy("balance_bucket")
)
