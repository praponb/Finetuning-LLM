from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.avg(F.col("t.amount")).alias("avg_amount"),
        (F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"),
    )
)
