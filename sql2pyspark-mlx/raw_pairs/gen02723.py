from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 85000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.percentile_approx("balance", 0.95).alias("balance_p95_2"),
    )
    .filter(F.col("balance_p95") >= 0)
    .orderBy(F.col("row_count").desc_nulls_first())
)
