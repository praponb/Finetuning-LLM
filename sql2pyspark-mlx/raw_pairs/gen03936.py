from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type").isin(["CREDIT", "LOAN"]))
    .groupBy(F.col("a.status"))
    .agg(F.countDistinct(F.col("t.amount")).alias("unique_accounts"))
    .filter(F.col("unique_accounts") > 5)
    .orderBy(F.col("status").desc(), F.col("unique_accounts").desc())
    .limit(1)
)
