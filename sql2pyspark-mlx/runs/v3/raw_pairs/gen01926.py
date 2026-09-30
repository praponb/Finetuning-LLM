from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.balance") >= 32000)
    .groupBy(
        F.when(F.year(F.col("a.opened_date")) < 2024, "before_2024").when(F.year(F.col("a.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.col("a.account_type"),
    )
    .agg(F.sum(F.col("t.amount")).alias("total_amount"))
)
