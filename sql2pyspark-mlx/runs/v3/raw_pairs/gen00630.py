from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .orderBy(F.col("balance_range").desc())
)
