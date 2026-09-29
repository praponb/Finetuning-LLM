from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_device_by_played = Window.partitionBy("is_skipped", "device").orderBy(F.col("played_at").desc(), "id")
w_is_skipped_device_by_played_2 = Window.partitionBy("is_skipped", "device").orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, -1)
w_all_by_ms_played_ties = Window.orderBy(F.col("ms_played").desc_nulls_first())

valid_plays = df_plays.filter(F.col("source") == "album")

ranked = (
    valid_plays
    .select(
        "*",
        F.row_number().over(w_is_skipped_device_by_played).alias("rn"),
        F.sum("ms_played").over(w_is_skipped_device_by_played_2).alias("ms_played_sum_6_prior"),
        F.dense_rank().over(w_all_by_ms_played_ties).alias("ms_played_dense_rank"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

profiled = (
    ranked.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.first_played_at")
)

result = (
    profiled
    .rollup("ms_played_dense_rank", "rn")
    .agg(
        F.round(F.sum("ms_played_dense_rank") / F.nullif(F.sum("ms_played_sum_6_prior"), F.lit(0)), 3).alias("ms_played_dense_rank_to_ms_played_sum_6_prior_ra"),
    )
    .orderBy(
        F.col("ms_played_dense_rank").asc_nulls_last(),
        F.col("rn").asc_nulls_last(),
    )
)
