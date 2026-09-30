from pyspark.sql import functions as F

valid_accounts = df_accounts.filter((F.col("balance") > 34000) | (F.col("customer_id") == 30))

summary = (
    valid_accounts
    .groupBy(
        "customer_id",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.count("balance").alias("balance_count"),
        F.count("*").alias("total_count"),
    )
)

result = (
    summary
    .select("customer_id", "opened_cohort", "balance_count", "total_count")
    .orderBy(F.col("balance_count").desc())
)
