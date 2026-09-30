from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("balance") > 57000) & F.col("balance").between(41000, 45000))
)

accounts_stats = (
    filtered_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.avg("balance").alias("avg_balance"))
)

result = accounts_stats.select("opened_cohort", "avg_balance")
