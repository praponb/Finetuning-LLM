from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type") != "LOAN")
    .groupBy(F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
    )
    .filter(F.col("pct_amazon") > 43)
)
