from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("balance") >= 65000)
    .withColumn("balance_whole", F.floor("balance"))
)

agg = (
    recent_accounts
    .groupBy("account_type")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    agg
    .orderBy(F.col("account_type").desc())
    .limit(20)
)
