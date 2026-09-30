from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.amount").between(2000, 2300))
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.count(F.col("t.amount")).alias("amount_count"),
        F.min(F.col("t.amount")).alias("min_amount"),
    )
    .filter(F.col("amount_count") >= 3)
    .orderBy("opened_year")
)
