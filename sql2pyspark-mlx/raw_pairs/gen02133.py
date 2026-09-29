from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("checking_count") < 5)
    .orderBy("min_balance")
)
