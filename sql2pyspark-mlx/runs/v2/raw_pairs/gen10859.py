from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
    .filter(F.col("pct_open") >= 24)
)
