from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_ms_played_full = Window.partitionBy("source").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source = Window.partitionBy("source")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_source_by_ms_played_prior_2 = Window.partitionBy("source").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-2, -1)
w_source_by_ms_played_full_2 = Window.partitionBy("source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_2 = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_by_ms_played = Window.partitionBy("source").orderBy(F.col("ms_played").desc(), "id")
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_source_by_ms_played_moving_3 = Window.partitionBy("source").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, Window.currentRow)

prepared = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
    )
)

windowed = (
    prepared
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "source",
        F.nth_value("ms_played", 2).over(w_source_by_ms_played_full).alias("second_ms_played"),
        F.sum("ms_played").over(w_source).alias("source_sum_ms_played"),
        F.lead("ms_played", 2, 0).over(w_all_by_ms_played).alias("next2_ms_played"),
        F.round(F.avg("ms_played").over(w_source_by_ms_played_prior_2), 2).alias("ms_played_avg_2_prior"),
        F.nth_value("ms_played", 2).over(w_source_by_ms_played_full_2).alias("second_ms_played_2"),
        F.row_number().over(w_all_by_ms_played_2).alias("ms_played_seq"),
        F.first("ms_played").over(w_all_by_ms_played_full).alias("first_ms_played"),
        F.lead("ms_played").over(w_source_by_ms_played).alias("next_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_listener_id), F.lit(0)), 4).alias("ms_played_share_of_track_id_listener_id"),
        F.round(F.avg("ms_played").over(w_source_by_ms_played_moving_3), 2).alias("ms_played_avg_4"),
    )
)

first_rows = (
    windowed
    .filter(F.col("ms_played_seq") <= 2)
    .drop("ms_played_seq")
)

per_group = (
    first_rows
    .groupBy("source", "track_id", "listener_id")
    .agg(
        F.max("ms_played").alias("max_ms_played"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("source") == "playlist", 1)).alias("n_source_playlist"),
        F.countDistinct("source").alias("distinct_sources"),
        F.count(F.when(F.col("source").isin("search", "radio", "album"), 1)).alias("n_source_in_search_radio_album"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    per_group
    .filter(~F.col("source").isin("playlist", "radio"))
    .select(
        "source",
        "track_id",
        "listener_id",
        "max_ms_played",
        "distinct_sources",
        "n_source_in_search_radio_album",
        F.when(F.col("n_source_playlist") >= 8, "HIGH").when(F.col("n_source_playlist") >= 3, "MEDIUM").otherwise("LOW").alias("n_source_playlist_tier"),
        F.greatest(F.col("distinct_sources"), F.col("cnt")).alias("greatest_distinct_sources_cnt"),
    )
    .orderBy("track_id")
)
