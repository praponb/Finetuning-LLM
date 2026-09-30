from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_played_at_ties = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc())
w_track_id = Window.partitionBy("track_id")
w_listener_id = Window.partitionBy("listener_id")
w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc(), "id")
w_listener_id_by_ms_played_pri = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, -1)
w_is_skipped_is_skipped_label = Window.partitionBy("is_skipped", "is_skipped_label").orderBy("track_duration_sec", "id")
w_is_skipped_label_next3_playe = Window.partitionBy("is_skipped_label", "next3_played_at_year")
w_next3_played_at_year_track_i = Window.partitionBy("next3_played_at_year", "track_id").orderBy(F.col("track_bpm").desc(), "id")
w_all_by_track_duration_sec_sh = Window.orderBy(F.col("track_duration_sec_share_of_track_id").desc(), "id")

base_plays = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(8, 14))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.year(F.col("played_at")).alias("played_at_year"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

joined_track = (
    base_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.played_at_year",
        "s.is_skipped_label",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    joined_track
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_skipped_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_track_id), F.lit(0)), 4).alias("track_duration_sec_share_of_track_id"),
        F.min("played_at_year").over(w_track_id).alias("track_id_min_played_at_year"),
        F.sum("played_at_year").over(w_listener_id).alias("listener_id_sum_played_at_year"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        (F.col("track_duration_sec") - F.lag("track_duration_sec").over(w_is_skipped_by_played_at)).alias("track_duration_sec_change"),
        F.max("track_bpm").over(w_listener_id_by_ms_played_pri).alias("track_bpm_max_6_prior"),
        F.lead("played_at_year", 3).over(w_is_skipped_is_skipped_label).alias("next3_played_at_year"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.size(F.collect_set("artist_country").over(w_is_skipped_label_next3_playe)).alias("distinct_artist_country_in_is_skipped_label_next"),
        F.ntile(3).over(w_next3_played_at_year_track_i).alias("track_bpm_tercile"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_is_skipped_label_next3_playe), F.lit(0)), 4).alias("track_duration_sec_share_of_is_skipped_label_nex"),
        F.round(F.col("track_duration_sec_change") / F.nullif(F.sum("track_duration_sec_change").over(w_is_skipped_label_next3_playe), F.lit(0)), 4).alias("track_duration_sec_change_share_of_is_skipped_la"),
        F.lag("next3_played_at_year", 2).over(w_all_by_track_duration_sec_sh).alias("prev2_next3_played_at_year"),
    )
)

grouped = (
    metrics
    .groupBy("distinct_artist_country_in_is_skipped_label_next")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    grouped
    .select("distinct_artist_country_in_is_skipped_label_next", "distinct_ids")
    .orderBy(
        F.col("distinct_ids").asc(),
        F.col("distinct_artist_country_in_is_skipped_label_next").asc_nulls_last(),
    )
    .limit(10)
)
