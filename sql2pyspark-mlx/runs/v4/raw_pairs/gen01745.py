from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & (F.col("opened_date") > "2024-08-24"))
)

agg = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
)

result = (
    agg
    .filter(F.col("pct_savings") < 48)
    .select("account_type", "pct_savings")
)
