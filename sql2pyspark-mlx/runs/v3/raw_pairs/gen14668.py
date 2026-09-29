from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([5, 7, 9]))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
    )
)
