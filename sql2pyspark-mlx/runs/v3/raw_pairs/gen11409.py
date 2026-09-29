from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("t.account_id") == 28)
    .groupBy(
        F.when(F.col("c.age") >= 69, "high").when(F.col("c.age") >= 38, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.count(F.when(F.col("t.merchant") == "Shell", 1)).alias("shell_count"),
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
    .orderBy(F.col("shell_count").desc())
)
