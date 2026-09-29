from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 56000) | F.col("balance").between(22000, 59000))
    .groupBy(
        "account_type",
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .orderBy(F.col("account_type").desc(), "status_group")
    .limit(1)
)
