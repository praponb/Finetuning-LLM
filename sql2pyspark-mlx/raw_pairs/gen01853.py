from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.amount").between(3100, 3200)), "left_anti")
)

result = (
    kept
    .select("id", "balance", "opened_date", "status")
    .orderBy(F.col("id").desc(), F.col("status").desc())
    .limit(10)
)
