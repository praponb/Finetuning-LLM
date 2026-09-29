from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_device = Window.partitionBy("played_at_year", "device")
w_listener_country_listener_pl = Window.partitionBy("listener_country", "listener_plan")
w_track_is_explicit_by_track_d = Window.partitionBy("track_is_explicit").orderBy(F.col("track_duration_sec").desc(), "id")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_track_is_explicit_by_track_d_2 = Window.partitionBy("track_is_explicit").orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_country_source_by_p = Window.partitionBy("listener_country", "source").orderBy("played_at", "id")
w_all_by_min_last_played_at_ye = Window.orderBy("min_last_played_at_year", "track_is_explicit", "listener_country").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_source_search_min_last_pla = Window.partitionBy("n_source_search", "min_last_played_at_year")

valid_plays = (
    df_plays
    .filter(F.col("source") != "playlist")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.year(F.col("played_at")).alias("played_at_year"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

with_parent = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("l.plan") == "family")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.played_at_year",
        "s.device_slug",
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

windowed = (
    with_parent
    .select(
        "*",
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_played_at_year_device), 2).alias("track_duration_sec_vs_played_at_year_device_avg"),
        F.round(F.col("played_at_year") / F.nullif(F.sum("played_at_year").over(w_played_at_year_device), F.lit(0)), 4).alias("played_at_year_share_of_played_at_year_device"),
        F.size(F.collect_set("device").over(w_listener_country_listener_pl)).alias("distinct_device_in_listener_country_listener_pla"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_listener_country_listener_pl), F.lit(0)), 4).alias("track_duration_sec_share_of_listener_country_lis"),
        F.dense_rank().over(w_track_is_explicit_by_track_d).alias("track_duration_sec_dense_rank"),
        F.size(F.collect_set("listener_id").over(w_played_at_year_device)).alias("distinct_listener_id_in_played_at_year_device"),
        F.lead("played_at", 3).over(w_all_by_played_at).alias("next3_played_at"),
        F.max("played_at_year").over(w_listener_country_listener_pl).alias("listener_country_listener_plan_max_played_at_yea"),
        F.count(F.lit(1)).over(w_played_at_year_device).alias("rows_in_played_at_year_device"),
        F.last("played_at_year").over(w_track_is_explicit_by_track_d_2).alias("last_played_at_year"),
        F.nth_value("played_at", 2).over(w_track_is_explicit_by_track_d_2).alias("second_played_at"),
        F.lag("played_at").over(w_listener_country_source_by_p).alias("prev_played_at"),
    )
)

top_rows = windowed.filter(F.col("track_duration_sec_dense_rank") <= 3)

stats = (
    top_rows
    .groupBy("track_is_explicit", "listener_country")
    .agg(
        F.percentile_approx("played_at_year", 0.5).alias("median_played_at_year"),
        F.countDistinct("listener_country").alias("distinct_listener_countrys"),
        F.count(F.when(F.col("source") == "search", 1)).alias("n_source_search"),
        F.sum(F.when(~F.col("listener_plan").isin("student", "free"), F.col("track_duration_sec_vs_played_at_year_device_avg"))).alias("track_duration_sec_vs_played_at_year_device_avg_"),
        F.min("last_played_at_year").alias("min_last_played_at_year"),
        F.min("second_played_at").alias("first_second_played_at"),
    )
)

sequenced = (
    stats
    .select(
        "track_is_explicit",
        "listener_country",
        "median_played_at_year",
        "n_source_search",
        "track_duration_sec_vs_played_at_year_device_avg_",
        "min_last_played_at_year",
        F.sum("n_source_search").over(w_all_by_min_last_played_at_ye).alias("running_n_source_search"),
        F.round(F.col("distinct_listener_countrys") - F.avg("distinct_listener_countrys").over(w_n_source_search_min_last_pla), 2).alias("distinct_listener_countrys_vs_n_source_search_mi"),
        F.sum("distinct_listener_countrys").over(w_n_source_search_min_last_pla).alias("n_source_search_min_last_played_at_year_sum_dist"),
    )
)

result = (
    sequenced
    .select(
        "track_is_explicit",
        "listener_country",
        "n_source_search",
        "track_duration_sec_vs_played_at_year_device_avg_",
        "running_n_source_search",
        "distinct_listener_countrys_vs_n_source_search_mi",
        "n_source_search_min_last_played_at_year_sum_dist",
        F.abs(F.col("median_played_at_year") - 2025).alias("median_played_at_year_distance"),
        F.when(F.col("min_last_played_at_year") >= 2026, "A").when(F.col("min_last_played_at_year") >= 2024, "B").otherwise("C").alias("min_last_played_at_year_tier"),
    )
    .orderBy(
        "listener_country",
        F.col("track_duration_sec_vs_played_at_year_device_avg_").desc_nulls_first(),
    )
)
