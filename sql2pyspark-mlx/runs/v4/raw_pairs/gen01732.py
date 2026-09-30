from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("t.is_flagged"))
    .groupBy(
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"),
        F.col("a.account_type"),
    )
    .agg(
        F.sum(F.when(F.col("t.merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
        F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"),
    )
    .orderBy("opened_month")
)
