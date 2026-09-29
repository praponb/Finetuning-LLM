from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("status", "account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") < 426000)
    .orderBy(F.col("total_balance").desc())
)
