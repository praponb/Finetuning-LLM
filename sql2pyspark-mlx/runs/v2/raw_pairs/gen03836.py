from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.opened_date").between("2023-07-22", "2024-12-01"))
    .groupBy(
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"),
        F.col("a.account_type"),
    )
    .agg((F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"))
)
