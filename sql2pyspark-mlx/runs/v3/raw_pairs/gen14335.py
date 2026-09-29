from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(34000, 74000))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
    )
    .filter(F.col("pct_checking") > 71)
    .select("status", "pct_checking")
)
