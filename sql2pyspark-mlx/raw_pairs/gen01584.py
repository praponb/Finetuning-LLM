from pyspark.sql import functions as F

filtered_accounts = df_accounts.filter(F.month("opened_date").isin([3, 6, 11]))

accounts_stats = (
    filtered_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.when(F.col("balance") >= 66000, "high").when(F.col("balance") >= 31000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = (
    accounts_stats
    .select("opened_cohort", "balance_bucket", "first_opened_date")
    .orderBy("first_opened_date")
)
