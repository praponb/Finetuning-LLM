from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 40)
    .groupBy("account_type", F.month("opened_date").alias("opened_month"))
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .orderBy("opened_month")
)
