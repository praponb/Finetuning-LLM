from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(
        F.month(F.col("a.opened_date")).alias("opened_month"),
        F.col("a.customer_id"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
    )
)
