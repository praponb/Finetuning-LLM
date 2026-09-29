from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(20000, 76000) | F.col("customer_id").isNotNull())
    .groupBy(
        F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)
