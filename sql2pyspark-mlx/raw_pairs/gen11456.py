from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
    )
    .orderBy("cnt")
)
