from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 8, 18, 24]) & (F.col("opened_date").between("2024-12-01", "2025-09-13") | F.col("customer_id").isin([1, 8, 25, 26])))
)

status_summary = (
    base
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
    )
)

result = (
    status_summary
    .select("status", "open_count", "savings_count")
    .orderBy(F.col("status").desc())
    .limit(10)
)
