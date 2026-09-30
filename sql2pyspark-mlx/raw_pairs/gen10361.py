from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy(F.year("opened_date"), "status").orderBy(F.col("balance").asc_nulls_last())
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_first())
w_part_2 = Window.orderBy(F.col("balance").desc_nulls_last())
w4 = Window.partitionBy(F.year("opened_date"), "status").orderBy(F.col("balance").desc())
w5 = Window.partitionBy("account_type")
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("status").orderBy(F.col("status_stddev_samp_balance").desc())
w8 = Window.partitionBy("account_type").orderBy(F.col("balance_rank_3").desc_nulls_last())

base = (
    df_accounts
    .filter(~F.col("status").eqNullSafe(F.lit("FROZEN")))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

kept_keys = (
    base
    .select("status", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("status") == "CLOSED")
        .select("status", "account_type")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.cume_dist().over(w_part).alias("balance_cume_dist"),
        F.rank().over(w2).alias("balance_rank"),
        F.rank().over(w_part_2).alias("balance_rank_2"),
        F.rank().over(w4).alias("balance_rank_3"),
    )
)

scored = (
    enriched
    .withColumn("balance_rank_2_share", F.round((F.col("balance_rank_2") / F.sum("balance_rank_2").over(w5)), 4))
)

metrics = (
    scored
    .filter(F.col("balance_rank") <= 6)
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "account_type_title",
        "status_clean",
        "balance_rank",
        "balance_rank_2",
        "balance_rank_3",
        "balance_rank_2_share",
        F.stddev_samp("balance").over(w6).alias("status_stddev_samp_balance"),
    )
)

enriched_accounts = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w7).alias("status_stddev_samp_balance_dense_rank"),
        F.count("status_stddev_samp_balance").over(w5).alias("account_type_count_status_stddev_samp_balance"),
        F.rank().over(w8).alias("balance_rank_3_rank"),
    )
)

stats = (
    enriched_accounts
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("balance_rank_2_share", 0.95).alias("balance_rank_2_share_p95"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "week_start",
        "num_rows",
        "num_status",
        "balance_rank_2_share_p95",
    )
    .orderBy(F.col("num_status").asc_nulls_last())
)
