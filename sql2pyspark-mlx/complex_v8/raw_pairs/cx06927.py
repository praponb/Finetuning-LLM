from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_country_track_id_by = Window.partitionBy("listener_country", "track_id").orderBy("played_at_dayofweek", "id")
w_track_id_by_track_duration_s = Window.partitionBy("track_id").orderBy("track_duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_source_group_track_id_by_pla = Window.partitionBy("source_group", "track_id").orderBy("played_at_dayofweek", "id")
w_track_id_by_track_duration_s_2 = Window.partitionBy("track_id").orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_group_track_id_by_pla_2 = Window.partitionBy("source_group", "track_id").orderBy("played_at_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_group_track_id_by_pla_3 = Window.partitionBy("source_group", "track_id").orderBy("played_at_dayofweek")
w_source_group = Window.partitionBy("source_group")
w_device = Window.partitionBy("device")
w_prev2_played_at_dayofweek_fi = Window.partitionBy("prev2_played_at_dayofweek", "first_known_played_at_dayofweek")
w_all_by_listener_birth_year_s = Window.orderBy("listener_birth_year_share_of_device")
w_played_at_dayofweek_tercile = Window.partitionBy("played_at_dayofweek_tercile").orderBy("played_at_dayofweek_percent_rank", "id")
w_played_at_dayofweek_by_ms_pl = Window.partitionBy("played_at_dayofweek").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_played_at_dayofweek_b = Window.partitionBy("device", "played_at_dayofweek").orderBy("listener_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_device_speaker_by_total_li = Window.partitionBy("n_device_speaker").orderBy("total_listener_birth_year_share_of_source_group", "listener_birth_year_tier", "track_id")
w_listener_birth_year_tier_tra = Window.partitionBy("listener_birth_year_tier", "track_id")

scoped = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("ms_played") <= 236512))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("album", "playlist"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
    )
)

with_parent_listener = (
    scoped.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(F.col("l.birth_year") > 1966)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.source_group",
        "s.played_at_dayofweek",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.country").alias("listener_country"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

sequenced = (
    with_parent_listener
    .select(
        "*",
        F.lead("ms_played", 1, 0).over(w_listener_country_track_id_by).alias("next_ms_played"),
        F.ntile(3).over(w_listener_country_track_id_by).alias("played_at_dayofweek_tercile"),
        F.round(F.avg("track_bpm").over(w_track_id_by_track_duration_s), 2).alias("track_bpm_avg_3"),
        F.lag("played_at_dayofweek", 2, 0).over(w_source_group_track_id_by_pla).alias("prev2_played_at_dayofweek"),
        F.nth_value("played_at_dayofweek", 2).over(w_track_id_by_track_duration_s_2).alias("second_played_at_dayofweek"),
        F.first("played_at_dayofweek", ignorenulls=True).over(w_source_group_track_id_by_pla_2).alias("first_known_played_at_dayofweek"),
        F.round(F.percent_rank().over(w_source_group_track_id_by_pla_3), 3).alias("played_at_dayofweek_percent_rank"),
        F.sum("ms_played").over(w_source_group).alias("source_group_sum_ms_played"),
        F.min("listener_birth_year").over(w_source_group).alias("source_group_min_listener_birth_year"),
        F.round(F.col("played_at_dayofweek") - F.avg("played_at_dayofweek").over(w_device), 2).alias("played_at_dayofweek_vs_device_avg"),
        F.min("ms_played").over(w_device).alias("device_min_ms_played"),
        F.max("played_at_dayofweek").over(w_source_group).alias("source_group_max_played_at_dayofweek"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_device), F.lit(0)), 4).alias("listener_birth_year_share_of_device"),
        F.round(F.col("played_at_dayofweek") - F.avg("played_at_dayofweek").over(w_source_group), 2).alias("played_at_dayofweek_vs_source_group_avg"),
        F.round(F.percent_rank().over(w_source_group_track_id_by_pla), 3).alias("played_at_dayofweek_percent_rank_2"),
        F.round(F.cume_dist().over(w_listener_country_track_id_by), 3).alias("played_at_dayofweek_cume_dist"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.max("track_bpm_avg_3").over(w_prev2_played_at_dayofweek_fi).alias("prev2_played_at_dayofweek_first_known_played_at_"),
        F.dense_rank().over(w_all_by_listener_birth_year_s).alias("listener_birth_year_share_of_device_dense_rank"),
        F.rank().over(w_played_at_dayofweek_tercile).alias("played_at_dayofweek_percent_rank_rank"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_source_group), F.lit(0)), 4).alias("listener_birth_year_share_of_source_group"),
        F.first("ms_played").over(w_played_at_dayofweek_by_ms_pl).alias("first_ms_played"),
        F.round(F.avg("track_bpm_avg_3").over(w_source_group), 2).alias("source_group_avg_track_bpm_avg_3"),
        F.nth_value("track_bpm_avg_3", 2).over(w_device_played_at_dayofweek_b).alias("second_track_bpm_avg_3"),
    )
)

totals = (
    with_trends
    .groupBy(
        F.when(F.col("listener_birth_year") >= 1986, "large").when(F.col("listener_birth_year") >= 1975, "medium").otherwise("small").alias("listener_birth_year_tier"),
        "track_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("device") == "speaker", 1)) / F.count("*"), 1).alias("pct_device_speaker"),
        F.count(F.when(F.col("device") == "speaker", 1)).alias("n_device_speaker"),
        F.min("track_bpm_avg_3").alias("min_track_bpm_avg_3"),
        F.array_sort(F.collect_set("listener_country")).alias("listener_country_list"),
        F.round(F.sum("listener_birth_year_share_of_source_group"), 2).alias("total_listener_birth_year_share_of_source_group"),
    )
)

windowed = (
    totals
    .select(
        "listener_birth_year_tier",
        "track_id",
        "min_track_bpm_avg_3",
        "listener_country_list",
        F.dense_rank().over(w_n_device_speaker_by_total_li).alias("total_listener_birth_year_share_of_source_group_"),
        F.round(F.col("pct_device_speaker") - F.avg("pct_device_speaker").over(w_listener_birth_year_tier_tra), 2).alias("pct_device_speaker_vs_listener_birth_year_tier_t"),
        F.min("pct_device_speaker").over(w_listener_birth_year_tier_tra).alias("listener_birth_year_tier_track_id_min_pct_device"),
        F.round(F.col("min_track_bpm_avg_3") / F.nullif(F.sum("min_track_bpm_avg_3").over(w_listener_birth_year_tier_tra), F.lit(0)), 4).alias("min_track_bpm_avg_3_share_of_listener_birth_year"),
    )
)

result = (
    windowed
    .select(
        "listener_birth_year_tier",
        "listener_country_list",
        "listener_birth_year_tier_track_id_min_pct_device",
        "min_track_bpm_avg_3_share_of_listener_birth_year",
        F.when(F.col("min_track_bpm_avg_3") >= 162.0, "large").when(F.col("min_track_bpm_avg_3") >= 84.0, "medium").otherwise("small").alias("min_track_bpm_avg_3_tier"),
    )
    .orderBy("min_track_bpm_avg_3_tier")
)
