from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_play_listener_id_by_is_expli = Window.partitionBy("play_listener_id").orderBy("is_explicit_flag")
w_play_listener_id_by_is_expli_2 = Window.partitionBy("play_listener_id").orderBy("is_explicit_flag", "id", "play_id")
w_play_listener_id_by_is_expli_3 = Window.partitionBy("play_listener_id").orderBy("is_explicit_flag", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

exploded_detail = (
    recent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isin("phone", "desktop")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.duration_sec_filled",
        "s.title_ref",
        "s.is_explicit_flag",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

with_windows = (
    exploded_detail
    .select(
        "*",
        F.round(F.avg("duration_sec_filled").over(w_release_date_month), 2).alias("release_date_month_avg_duration_sec_filled"),
        F.round(F.cume_dist().over(w_play_listener_id_by_is_expli), 3).alias("is_explicit_flag_cume_dist"),
        F.ntile(3).over(w_play_listener_id_by_is_expli_2).alias("is_explicit_flag_tercile"),
        F.first("duration_sec_filled").over(w_play_listener_id_by_is_expli_3).alias("first_duration_sec_filled"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_release_date_month), 2).alias("bpm_vs_release_date_month_avg"),
    )
)

play_source_stats = (
    with_windows
    .groupBy("play_source")
    .agg(F.percentile_approx("bpm_vs_release_date_month_avg", 0.5).alias("median_bpm_vs_release_date_month_avg"))
)

vs_group = (
    with_windows.alias("s")
    .join(play_source_stats.alias("g"), F.col("g.play_source") == F.col("s.play_source"), "inner")
    .filter(F.col("s.bpm_vs_release_date_month_avg") >= F.col("g.median_bpm_vs_release_date_month_avg"))
    .select(
        "s.*",
        "g.median_bpm_vs_release_date_month_avg",
        F.round(F.col("s.bpm_vs_release_date_month_avg") - F.col("g.median_bpm_vs_release_date_month_avg"), 2).alias("bpm_vs_release_date_month_avg_minus_median"),
    )
)

with_children = (
    vs_group.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.title_ref",
        "s.is_explicit_flag",
        "s.play_id",
        "s.play_listener_id",
        "s.release_date_month_avg_duration_sec_filled",
        "s.is_explicit_flag_tercile",
        "s.first_duration_sec_filled",
        "s.bpm_vs_release_date_month_avg",
        "s.median_bpm_vs_release_date_month_avg",
        "s.bpm_vs_release_date_month_avg_minus_median",
        F.col("p.id").alias("play_id_2"),
        F.col("p.listener_id").alias("play_listener_id_2"),
        F.col("p.source").alias("play_source_2"),
    )
)

filtered = (
    with_children
    .crossJoin(with_children.agg(F.avg("release_date_month_avg_duration_sec_filled").alias("overall_avg_release_date_month_avg_duration_sec_")))
    .filter(F.col("release_date_month_avg_duration_sec_filled") > F.col("overall_avg_release_date_month_avg_duration_sec_"))
    .drop("overall_avg_release_date_month_avg_duration_sec_")
)

result = (
    filtered
    .filter(F.col("is_explicit_flag") >= 0)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "title_ref",
        "play_id",
        "play_listener_id",
        "release_date_month_avg_duration_sec_filled",
        "is_explicit_flag_tercile",
        "first_duration_sec_filled",
        "bpm_vs_release_date_month_avg",
        "median_bpm_vs_release_date_month_avg",
        "bpm_vs_release_date_month_avg_minus_median",
        "play_id_2",
        "play_listener_id_2",
        "play_source_2",
        F.least(F.col("duration_sec"), F.col("is_explicit_flag")).alias("least_duration_sec_is_explicit_flag"),
        F.concat_ws("-", F.col("play_source_2"), F.col("title_ref")).alias("play_source_2_title_ref_label"),
    )
    .orderBy("play_source_2", F.col("play_source_2_title_ref_label").desc())
)
