from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_play_device_count = Window.orderBy(F.col("play_device_count").desc(), "id")
w_device_play_device_count = Window.partitionBy("device", "play_device_count")
w_device_track_id_by_play_coun = Window.partitionBy("device", "track_id").orderBy(F.col("play_count_filled").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_play_count = Window.partitionBy("play_count")
w_play_count_by_play_device_co = Window.partitionBy("play_count").orderBy("play_device_count_quintile")
w_all_by_device_play_device_co = Window.orderBy("device_play_device_count_min_play_device_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_device_count_quintile_r = Window.partitionBy("play_device_count_quintile_rank", "source")
w_play_count_filled_by_rows_in = Window.partitionBy("play_count_filled").orderBy(F.col("rows_in_device_play_device_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_device_play_device_co_2 = Window.orderBy("device_play_device_count_min_play_device_count")

cleaned = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("source") != "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("device").isin("phone", "car"), 1)).alias("play_device_count"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

tagged = (
    summarized_rows
    .select(
        "*",
        F.coalesce(F.col("play_count"), F.lit(0)).alias("play_count_filled"),
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "ms_played",
        "device",
        "source",
        "max_play_ms_played",
        "play_count",
        "play_count_filled",
        "source_device_label",
        F.ntile(5).over(w_all_by_play_device_count).alias("play_device_count_quintile"),
        F.count(F.lit(1)).over(w_device_play_device_count).alias("rows_in_device_play_device_count"),
        F.min("play_device_count").over(w_device_play_device_count).alias("device_play_device_count_min_play_device_count"),
        F.min("ms_played").over(w_device_play_device_count).alias("device_play_device_count_min_ms_played"),
        F.round(F.avg("ms_played").over(w_device_track_id_by_play_coun), 2).alias("ms_played_avg_4"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.max("play_device_count_quintile").over(w_play_count).alias("play_count_max_play_device_count_quintile"),
        F.rank().over(w_play_count_by_play_device_co).alias("play_device_count_quintile_rank"),
        F.size(F.collect_set("device").over(w_play_count)).alias("distinct_device_in_play_count"),
    )
)

tagged_2 = (
    metrics
    .withColumn("play_count_filled_tier", F.when(F.col("play_count_filled") >= 7, "large").when(F.col("play_count_filled") >= 2, "medium").otherwise("small"))
)

with_windows = (
    tagged_2
    .select(
        "*",
        F.nth_value("play_count", 2).over(w_all_by_device_play_device_co).alias("second_play_count"),
        F.size(F.collect_set("device").over(w_play_device_count_quintile_r)).alias("distinct_device_in_play_device_count_quintile_ra"),
        F.nth_value("max_play_ms_played", 2).over(w_play_count_filled_by_rows_in).alias("second_max_play_ms_played"),
        F.dense_rank().over(w_all_by_device_play_device_co_2).alias("device_play_device_count_min_play_device_count_d"),
        F.nth_value("ms_played", 2).over(w_all_by_device_play_device_co).alias("second_ms_played"),
        F.count(F.lit(1)).over(w_play_device_count_quintile_r).alias("rows_in_play_device_count_quintile_rank_source"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "ms_played",
        "device",
        "source",
        "max_play_ms_played",
        "play_count_filled",
        "play_device_count_quintile",
        "rows_in_device_play_device_count",
        "device_play_device_count_min_play_device_count",
        "ms_played_avg_4",
        "play_count_max_play_device_count_quintile",
        "play_device_count_quintile_rank",
        "distinct_device_in_play_count",
        "play_count_filled_tier",
        "second_play_count",
        "distinct_device_in_play_device_count_quintile_ra",
        "device_play_device_count_min_play_device_count_d",
        "second_ms_played",
        "rows_in_play_device_count_quintile_rank_source",
        F.round(F.col("ms_played_avg_4") * 0.9, 2).alias("ms_played_avg_4_adj"),
        F.concat_ws("|", F.col("source_device_label"), F.col("play_count_filled_tier")).alias("source_device_label_play_count_filled_tier_label"),
    )
    .orderBy(
        F.col("play_count_filled").desc(),
        F.col("play_device_count_quintile_rank").asc(),
    )
)
