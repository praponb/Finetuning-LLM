from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.min("balance").alias("min_balance"))
    .filter(F.col("min_balance") >= 28000)
)
