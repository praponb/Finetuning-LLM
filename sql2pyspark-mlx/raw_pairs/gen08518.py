from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("balance")
w4 = Window.partitionBy("account_id", "status")
w5 = Window.partitionBy("status")
w6 = Window.orderBy(F.col("balance_rank").desc_nulls_first())

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

kept_keys = (
    islands
    .select("id")
    .intersect(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id")
    )
)

reconciled = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select(
        "src.account_id",
        "src.amount",
        "src.is_flagged",
        "src.session_id",
        F.col("d.balance"),
        F.col("d.status"),
    )
)

scored = joined.select("*", F.rank().over(w3).alias("balance_rank"))

metrics = (
    scored
    .select(
        "session_id",
        "balance",
        "balance_rank",
        F.stddev_samp("balance_rank").over(w4).alias("account_id_stddev_samp_balance_rank"),
        F.stddev_samp("balance").over(w5).alias("status_stddev_samp_balance"),
        F.cume_dist().over(w6).alias("balance_rank_cume_dist"),
    )
)

base = (
    metrics
    .filter(F.col("session_id").between(4, 5))
    .select(
        "balance",
        "balance_rank",
        F.floor("balance_rank_cume_dist").alias("balance_rank_cume_dist_whole"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("balance_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("balance_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("balance_rank", [0.25, 0.5, 0.75]).alias("balance_rank_quartiles"),
        F.sum("balance_rank_cume_dist_whole").alias("total_balance_rank_cume_dist_whole"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("cnt").asc_nulls_last())
)
