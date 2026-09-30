from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 40000)
    .groupBy("customer_id", "account_type")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") < 55)
)
