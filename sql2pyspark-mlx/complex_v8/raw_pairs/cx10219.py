from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("source").orderBy(F.col("played_at_plus_1d").desc(), F.col("id").desc())
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_source_by_played_at_ties = Window.partitionBy("source").orderBy("played_at")
w_device_by_next_played_at = Window.partitionBy("device").orderBy(F.col("next_played_at").desc(), "id")
w_played_at_rank_by_played_at = Window.partitionBy("played_at_rank").orderBy("played_at", "id")
w_device_by_next_played_at_run = Window.partitionBy("device").orderBy("next_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .filter(F.col("track_id") <= 7)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
    )
)

unique_rows = (
    recent_plays
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    unique_rows
    .select(
        "*",
        F.lag("played_at").over(w_all_by_played_at).alias("prev_played_at"),
        F.lag("played_at_plus_1d", 2).over(w_source_by_played_at).alias("prev2_played_at_plus_1d"),
        F.lag("played_at_plus_1d").over(w_all_by_played_at).alias("prev_played_at_plus_1d"),
        F.dense_rank().over(w_source_by_played_at_ties).alias("played_at_dense_rank"),
        F.lead("played_at").over(w_source_by_played_at).alias("next_played_at"),
        F.rank().over(w_source_by_played_at_ties).alias("played_at_rank"),
    )
)

source_stats = (
    ranked
    .groupBy("source")
    .agg(F.max("played_at_dense_rank").alias("max_played_at_dense_rank"))
)

outliers = (
    ranked.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.played_at_dense_rank") == F.col("g.max_played_at_dense_rank"))
    .select(
        "s.*",
        "g.max_played_at_dense_rank",
        F.round(F.col("s.played_at_dense_rank") - F.col("g.max_played_at_dense_rank"), 2).alias("played_at_dense_rank_minus_max"),
    )
)

metrics = (
    outliers
    .select(
        "*",
        F.lead("prev_played_at").over(w_device_by_next_played_at).alias("next_prev_played_at"),
        F.round((F.unix_timestamp("prev_played_at") - F.unix_timestamp(F.lag("prev_played_at").over(w_played_at_rank_by_played_at))) / 60, 1).alias("minutes_since_prev_prev_played_at"),
        F.sum("played_at_rank").over(w_device_by_next_played_at_run).alias("running_played_at_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        "source_slug",
        "source_prefix",
        "prev2_played_at_plus_1d",
        "prev_played_at_plus_1d",
        "played_at_dense_rank",
        "next_played_at",
        "played_at_rank",
        "played_at_dense_rank_minus_max",
        "next_prev_played_at",
        "running_played_at_rank",
        F.substring(F.trim(F.col("source_prefix")), 1, 3).alias("source_prefix_prefix"),
    )
    .orderBy("source_prefix_prefix", "id")
    .limit(10)
)
