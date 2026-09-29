from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.round(((100 * F.sum(F.when(F.col("a.account_type") == "CHECKING", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checking"),
        F.countDistinct(F.col("a.balance")).alias("unique_customers"),
    )
    .filter(F.col("unique_customers") > 5)
)
