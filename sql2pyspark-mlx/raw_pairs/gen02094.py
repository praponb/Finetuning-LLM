from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"),
        F.col("a.customer_id"),
    )
    .agg(F.count(F.when(F.col("t.merchant") == "Grab", 1)).alias("grab_count"))
    .orderBy("grab_count", "opened_month")
)
