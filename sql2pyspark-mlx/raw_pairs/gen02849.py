from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "CHECKING"]) & F.col("status").isNotNull())
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
    )
    .filter(F.col("pct_checking") >= 74)
)
