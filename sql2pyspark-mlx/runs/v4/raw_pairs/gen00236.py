from pyspark.sql import functions as F

joined = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.account_type"), F.col("d.opened_date"))
)

result = (
    joined
    .groupBy(
        "account_type",
        F.date_trunc("quarter", "txn_time").alias("quarter_start"),
        F.date_trunc("year", "opened_date").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.min("txn_time").alias("started_at"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
