from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-06-01", "2024-11-01"))
    .groupBy(
        "status",
        F.when(F.col("balance") >= 85000, "high").when(F.col("balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.max("balance").alias("max_balance"))
    .filter(F.col("max_balance") < 28000)
)
