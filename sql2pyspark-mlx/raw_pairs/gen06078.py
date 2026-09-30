from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 78000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.count_if(F.col("account_type") == "LOAN").alias("loan_events"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
    .filter(F.col("loan_events") > 1)
)
