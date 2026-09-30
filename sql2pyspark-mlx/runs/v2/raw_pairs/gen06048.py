from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]) & F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .groupBy(
        "status",
        F.when(F.col("balance") >= 72000, "high").when(F.col("balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count("balance").alias("balance_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("balance_count") < 4)
)
