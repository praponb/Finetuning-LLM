from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.round(((100 * F.sum(F.when(F.col("a.account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"),
    )
    .filter(F.col("pct_credit") > 52)
)
