from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.age") >= 55, "high").when(F.col("c.age") >= 38, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
        F.sum(F.when(F.col("t.merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
    )
    .filter(F.col("7_eleven_count") > 21)
    .orderBy(F.col("pct_lazada").desc(), "age_bucket")
    .limit(10)
)
