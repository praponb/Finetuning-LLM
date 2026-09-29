from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "OPEN") & (F.col("balance") >= 70000))
    .withColumn("balance_rounded", F.round("balance", 2))
)
