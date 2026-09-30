from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(
        F.month(F.col("a.opened_date")).alias("opened_month"),
        F.year(F.col("a.opened_date")).alias("opened_year"),
    )
    .agg(
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
        F.countDistinct(F.col("t.amount")).alias("unique_merchants"),
    )
    .orderBy("opened_month")
)
