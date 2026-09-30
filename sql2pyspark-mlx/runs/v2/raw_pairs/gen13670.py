from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.country") == "SG")
    .groupBy(F.col("c.country"), F.col("c.city"))
    .agg(
        F.sum(F.col("a.balance")).alias("total_balance"),
        F.round(((100 * F.sum(F.when(F.col("a.account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .orderBy(F.col("pct_credit").desc(), "total_balance")
)
