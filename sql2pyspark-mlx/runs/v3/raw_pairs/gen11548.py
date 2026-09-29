from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(
        F.col("a.account_type"),
        F.year(F.col("a.opened_date")).alias("opened_year"),
    )
    .agg(
        F.sum(F.when(F.col("t.merchant") == "Starbucks", F.col("t.amount")).otherwise(0)).alias("starbucks_amount"),
        F.max(F.col("t.amount")).alias("max_amount"),
    )
    .orderBy("account_type", F.col("starbucks_amount").desc())
)
