from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") > F.lit("2023-10-31").cast("date")))
    .select(
        "*",
        F.length("txn_type").alias("txn_type_length"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

result = (
    base
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.status").alias("status"), F.col("s.txn_type").alias("txn_type"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("s.txn_type_length")), 2).alias("total_txn_type_length"),
        F.max(F.col("s.amount")).alias("max_amount"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
