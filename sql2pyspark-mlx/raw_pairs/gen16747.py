from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.status") == "OPEN")
    .groupBy(
        F.when(F.col("a.status").isin(["CLOSED", "OPEN"]), "core").otherwise("other").alias("status_group"),
        F.col("a.account_type"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
