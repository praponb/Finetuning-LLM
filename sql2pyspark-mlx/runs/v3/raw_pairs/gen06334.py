from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("opened_date") < "2025-09-24").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.account_id"),
        F.col("t.amount"),
    )
    .orderBy(F.col("account_id").desc(), "id")
    .limit(50)
)
