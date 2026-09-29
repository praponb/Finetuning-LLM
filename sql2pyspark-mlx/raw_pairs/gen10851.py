from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.balance") > 45000)
    .groupBy(F.col("c.country"))
    .agg(
        F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("a.status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
    .orderBy("avg_balance")
)
