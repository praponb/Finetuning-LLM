from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]) & F.month("opened_date").isin([7, 8, 12]) & (F.col("opened_date") < "2024-03-01"))
)

status_summary = (
    recent_accounts
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = (
    status_summary
    .filter(F.col("balance_range") > 77)
    .select("status", "frozen_count", "balance_range")
    .orderBy(F.col("frozen_count").desc())
)
