from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.opened_date") > "2025-06-01")
    .groupBy(F.col("a.status"))
    .agg(
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
        F.count("*").alias("num_transactions"),
    )
    .orderBy("status")
)
