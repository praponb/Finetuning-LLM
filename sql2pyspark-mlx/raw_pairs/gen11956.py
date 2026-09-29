from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("account_type") == "CREDIT") & (F.col("customer_id") == 14)))
    .groupBy("account_type", "status")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("balance_range") > 60)
    .orderBy("open_count", "status")
)
