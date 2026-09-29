from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.count("balance").alias("balance_count"),
    )
    .orderBy(F.col("balance_range").desc(), "account_type", "status_group")
    .limit(3)
)
