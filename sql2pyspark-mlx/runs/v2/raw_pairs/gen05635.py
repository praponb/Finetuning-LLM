from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]) & (F.year("opened_date") >= 2025) & F.col("customer_id").isin([5, 20, 22, 35]))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") >= 75)
)
