from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("balance") >= 76000) | F.col("opened_date").between("2023-06-01", "2023-09-09"))
)

summary = (
    filtered_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
)

result = (
    summary
    .filter(F.col("pct_open") < 60)
    .select("account_type", "open_count", "pct_open")
    .orderBy("pct_open")
)
