from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.min(F.col("a.balance")).alias("min_balance"),
        F.round(((100 * F.sum(F.when(F.col("a.status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
)
