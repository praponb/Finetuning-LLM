from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(
        F.col("a.status"),
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"),
    )
    .agg(F.count(F.when(F.col("t.merchant") == "Grab", 1)).alias("grab_count"))
    .orderBy(F.col("grab_count").desc(), "opened_month")
)
