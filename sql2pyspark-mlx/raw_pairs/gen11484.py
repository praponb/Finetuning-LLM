from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("balance").alias("min_balance"),
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
    )
    .orderBy(F.col("customer_id").desc())
    .limit(50)
)
