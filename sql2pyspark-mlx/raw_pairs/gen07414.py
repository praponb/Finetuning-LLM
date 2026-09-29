from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(
        F.col("a.account_type"),
        F.when(F.year(F.col("a.opened_date")) < 2024, "before_2024").when(F.year(F.col("a.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("t.merchant") == "Amazon", 1).otherwise(0)).alias("amazon_count"),
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
)
