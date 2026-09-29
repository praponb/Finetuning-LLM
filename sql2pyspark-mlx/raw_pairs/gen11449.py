from pyspark.sql import functions as F

base = df_accounts.filter(F.col("opened_date").between("2023-04-16", "2025-03-01"))

accounts_stats = (
    base
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
        F.avg("balance").alias("avg_balance"),
    )
)

result = (
    accounts_stats
    .select("opened_cohort", "opened_year", "pct_savings", "avg_balance")
    .orderBy(F.col("avg_balance").desc())
)
