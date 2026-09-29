from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("opened_date").between("2024-11-29", "2025-09-01"))

agg = (
    recent_accounts
    .groupBy(F.year("opened_date").alias("opened_year"), "status")
    .agg(
        F.min("balance").alias("min_balance"),
        F.max("opened_date").alias("last_opened_date"),
    )
)

result = (
    agg
    .filter(F.col("min_balance") >= 56000)
    .select("opened_year", "status", "min_balance", "last_opened_date")
    .orderBy(F.col("min_balance").desc())
)
