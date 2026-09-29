from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.max(F.col("t.amount")).alias("max_amount"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
    .orderBy("opened_year", "avg_amount")
)
