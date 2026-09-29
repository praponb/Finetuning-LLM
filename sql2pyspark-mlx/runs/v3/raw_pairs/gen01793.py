from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
    )
    .filter(F.col("avg_balance") >= 39)
    .select("opened_cohort", "avg_balance", "checking_count")
    .orderBy("avg_balance")
)
