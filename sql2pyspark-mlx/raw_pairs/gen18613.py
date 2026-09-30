from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 38000)
    .groupBy(
        "account_type",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        "status",
    )
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
    )
    .orderBy(F.col("pct_loan").desc())
)
