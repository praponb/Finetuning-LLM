from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy(F.date_trunc("MONTH", "txn_time"), "txn_type").orderBy(F.col("island_id").desc())
w_ordered = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy("island_id_dense_rank")
w7 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy(F.year("txn_time"), "txn_type").orderBy(F.col("island_id_dense_rank_sum_90d").desc_nulls_last())
w9 = Window.partitionBy(F.to_date("txn_time"), "txn_type")
w10 = Window.orderBy(F.col("island_id_dense_rank_cume_dist").asc_nulls_first())

valid = (
    df_transactions
    .filter(F.col("is_flagged") & (~F.col("merchant").eqNullSafe(F.lit("Shell"))))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

kept_keys = (
    islands
    .select("id")
    .intersect(
        df_transactions
        .filter(F.col("merchant").isNotNull())
        .select("id")
    )
)

reconciled = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = reconciled.select("*", F.dense_rank().over(w_grp).alias("island_id_dense_rank"))

scored = (
    enriched
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.sum("island_id_dense_rank").over(w_ordered).alias("island_id_dense_rank_sum_90d"),
        F.count("island_id").over(w5).alias("island_id_count_7d"),
        F.cume_dist().over(w6).alias("island_id_dense_rank_cume_dist"),
        F.sum("island_id_dense_rank").over(w_ordered).alias("island_id_dense_rank_sum_90d_2"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "island_id_dense_rank_sum_90d",
        "island_id_count_7d",
        "island_id_dense_rank_cume_dist",
        F.avg("island_id_dense_rank_sum_90d").over(w7).alias("island_id_dense_rank_sum_90d_avg_90d"),
        F.dense_rank().over(w8).alias("island_id_dense_rank_sum_90d_dense_rank"),
        F.round((F.col("island_id_dense_rank_sum_90d_2") / F.sum("island_id_dense_rank_sum_90d_2").over(w9)), 4).alias("island_id_dense_rank_sum_90d_2_share"),
    )
)

metrics = (
    with_windows
    .withColumn("island_id_dense_rank_cume_dist_percent_rank", F.percent_rank().over(w10))
)

result = (
    metrics
    .select(
        "id",
        "island_id_count_7d",
        "island_id_dense_rank_cume_dist",
        "island_id_dense_rank_sum_90d_avg_90d",
        "island_id_dense_rank_sum_90d_dense_rank",
        "island_id_dense_rank_sum_90d_2_share",
        "island_id_dense_rank_cume_dist_percent_rank",
    )
    .orderBy(F.col("island_id_dense_rank_cume_dist_percent_rank").desc_nulls_first())
)
