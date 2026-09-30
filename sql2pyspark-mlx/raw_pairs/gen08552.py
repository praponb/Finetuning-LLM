from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "LOAN"]) & (F.col("opened_date") <= "2025-04-01"))
)

summary = (
    recent_accounts
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
)

result = (
    summary
    .select("account_type", "first_opened_date", "pct_frozen")
    .orderBy("pct_frozen")
)
