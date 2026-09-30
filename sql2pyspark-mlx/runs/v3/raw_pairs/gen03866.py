from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("balance_count") >= 2)
    .orderBy("pct_closed", "account_type")
)
