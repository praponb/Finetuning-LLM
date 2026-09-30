from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("merchant")
w2 = Window.partitionBy("account_id").orderBy(F.col("is_flagged_flag").desc())

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2024-04-03"))) & F.col("merchant").isNotNull() & F.col("is_flagged"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("DIRECT")))
)

kept_keys = (
    cleaned
    .select("id", "merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "merchant")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.max("is_flagged_flag").over(w_grp).alias("merchant_max_is_flagged_flag"),
        F.dense_rank().over(w2).alias("is_flagged_flag_dense_rank"),
        F.min("is_flagged_flag").over(w_grp).alias("merchant_min_is_flagged_flag"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.opened_date"))
)

with_dims_transactions = (
    with_dims
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.account_type"), F.col("d.status"))
)

joined = (
    with_dims_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.balance"),
        F.col("b.status").alias("account_status"),
        F.col("b.opened_date").alias("account_opened_date"),
    )
)

result = (
    joined
    .withColumn("quarter_start", F.date_trunc("QUARTER", "txn_time"))
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .rollup("quarter_start", "merchant", "year_start")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("is_flagged_flag_dense_rank", [0.25, 0.5, 0.75]).alias("is_flagged_flag_dense_rank_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.count_if(F.col("account_status") == "FROZEN").alias("frozen_events"),
        F.grouping("quarter_start").alias("gid"),
    )
)
