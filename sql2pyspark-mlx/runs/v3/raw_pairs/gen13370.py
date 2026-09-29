from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isNull() & ~F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
)

agg = (
    base
    .groupBy(
        F.when(F.col("balance") >= 79000, "high").when(F.col("balance") >= 17000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)

result = (
    agg
    .select("balance_bucket", "stddev_balance")
    .orderBy("stddev_balance")
)
