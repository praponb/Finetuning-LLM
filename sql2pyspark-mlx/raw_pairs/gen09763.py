from pyspark.sql import functions as F

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("balance") >= 74000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("balance_per_checking"),
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
    )
    .orderBy(F.col("cnt_2").desc())
)
