from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)
