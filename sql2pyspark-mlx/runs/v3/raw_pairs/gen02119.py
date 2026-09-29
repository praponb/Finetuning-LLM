from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.account_type").isin(["LOAN", "SAVINGS", "CHECKING"]))
    .groupBy(
        F.month(F.col("a.opened_date")).alias("opened_month"),
        F.when(F.year(F.col("a.opened_date")) < 2024, "before_2024").when(F.year(F.col("a.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.count(F.col("t.amount")).alias("amount_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("amount_count").desc())
)
