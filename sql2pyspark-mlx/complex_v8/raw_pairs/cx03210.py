from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_track_id = Window.partitionBy("source", "track_id")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_listener_id_source_by_ms_pla = Window.partitionBy("listener_id", "source").orderBy("ms_played", "id")
w_track_id = Window.partitionBy("track_id")
w_listener_id_source_by_ms_pla_2 = Window.partitionBy("listener_id", "source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_source_by_ms_pla_3 = Window.partitionBy("listener_id", "source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_by_ms_played_prior_2 = Window.partitionBy("source").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(-2, -1)

filtered_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
    )
)

with_windows = (
    filtered_plays
    .select(
        "*",
        F.size(F.collect_set("listener_id").over(w_source_track_id)).alias("distinct_listener_id_in_source_track_id"),
        F.lead("ms_played").over(w_listener_id_by_ms_played).alias("next_ms_played"),
        F.lead("ms_played").over(w_listener_id_source_by_ms_pla).alias("next_ms_played_2"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source_track_id), F.lit(0)), 4).alias("ms_played_share_of_source_track_id"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id), F.lit(0)), 4).alias("ms_played_share_of_track_id"),
        F.sum("ms_played").over(w_listener_id_source_by_ms_pla_2).alias("running_ms_played"),
        F.nth_value("ms_played", 2).over(w_listener_id_source_by_ms_pla_3).alias("second_ms_played"),
        F.min("ms_played").over(w_track_id).alias("track_id_min_ms_played"),
        F.sum("ms_played").over(w_source_by_ms_played_prior_2).alias("ms_played_sum_2_prior"),
        F.ntile(3).over(w_listener_id_source_by_ms_pla).alias("ms_played_tercile"),
    )
)

totals = (
    with_windows
    .groupBy("is_skipped")
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.round(100 * F.count(F.when(F.col("source").isin("radio", "album", "search"), 1)) / F.count("*"), 1).alias("pct_source_in_radio_album_search"),
        F.min("track_id_min_ms_played").alias("min_track_id_min_ms_played"),
    )
)

result = (
    totals
    .filter(F.col("min_track_id_min_ms_played") <= 2601750)
    .select(
        "is_skipped",
        "distinct_listener_ids",
        "pct_source_in_radio_album_search",
        "min_track_id_min_ms_played",
        F.abs(F.col("distinct_listener_ids") - 7).alias("distinct_listener_ids_distance"),
    )
)
