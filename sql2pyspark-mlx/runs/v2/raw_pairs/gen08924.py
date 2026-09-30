from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(
        F.year(F.col("a.opened_date")).alias("opened_year"),
        F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(F.sum(F.col("t.amount")).alias("total_amount"))
)
