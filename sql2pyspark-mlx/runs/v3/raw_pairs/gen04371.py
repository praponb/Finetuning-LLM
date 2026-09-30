from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]))
    .groupBy("status")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)
