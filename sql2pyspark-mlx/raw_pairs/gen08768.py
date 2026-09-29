from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([3, 13]))
    .groupBy("account_type", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
    )
    .filter(F.col("pct_loan") >= 34)
)
