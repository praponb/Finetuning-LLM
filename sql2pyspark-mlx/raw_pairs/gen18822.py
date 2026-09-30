from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("a.opened_date")) < 2024, "before_2024").when(F.year(F.col("a.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
        F.month(F.col("a.opened_date")).alias("opened_month"),
    )
    .agg((F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"))
)
