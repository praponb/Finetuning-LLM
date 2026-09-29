from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.quarter(F.col("a.opened_date")).alias("opened_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "7-Eleven", 1).otherwise(0))) / F.count("*")), 1).alias("pct_7_eleven"),
    )
    .orderBy(F.col("pct_7_eleven").desc())
)
