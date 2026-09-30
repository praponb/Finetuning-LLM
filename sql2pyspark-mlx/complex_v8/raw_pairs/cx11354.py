from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_count_by_least_play_cou = Window.partitionBy("play_count").orderBy("least_play_count_bpm", "id").rowsBetween(-2, -1)
w_all_by_greatest_duration_sec = Window.orderBy(F.col("greatest_duration_sec_bpm").asc_nulls_last(), "id")
w_play_count_by_least_play_cou_2 = Window.partitionBy("play_count").orderBy("least_play_count_bpm", "id")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_seq_no = Window.orderBy(F.col("seq_no").desc(), "id")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm_avg_2_prior").asc_nulls_last())

filtered = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2024-12-22").cast("date")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
    )
)

tagged = (
    with_counts
    .select(
        "*",
        F.concat_ws("|", F.col("title"), F.col("title_prefix")).alias("title_title_prefix_label"),
        F.when(F.col("bpm_filled") >= 150, "A").when(F.col("bpm_filled") >= 90, "B").otherwise("C").alias("bpm_filled_tier"),
        F.least(F.col("play_count"), F.col("bpm")).alias("least_play_count_bpm"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        "title_prefix",
        "greatest_duration_sec_bpm",
        "play_count",
        F.round(F.avg("bpm").over(w_play_count_by_least_play_cou), 2).alias("bpm_avg_2_prior"),
        F.row_number().over(w_all_by_greatest_duration_sec).alias("seq_no"),
        F.lead("bpm_filled", 2).over(w_play_count_by_least_play_cou_2).alias("next2_bpm_filled"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_artist_id), 2).alias("seq_no_vs_artist_id_avg"),
        F.lag("greatest_duration_sec_bpm", 1, 0).over(w_all_by_seq_no).alias("prev_greatest_duration_sec_bpm"),
        (F.col("seq_no") - F.lag("seq_no").over(w_all_by_seq_no)).alias("seq_no_change"),
        F.dense_rank().over(w_artist_id_is_explicit_by_bpm).alias("bpm_avg_2_prior_dense_rank"),
    )
)

tagged_2 = (
    sequenced
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("title_prefix")), " ", "_")).alias("title_prefix_slug"),
        F.round(F.col("play_count") / F.nullif(F.col("next2_bpm_filled"), F.lit(0)), 3).alias("play_count_per_next2_bpm_filled"),
    )
)

artist_id_stats = (
    tagged_2
    .groupBy("artist_id")
    .agg(F.percentile_approx("prev_greatest_duration_sec_bpm", 0.5).alias("median_prev_greatest_duration_sec_bpm"))
)

vs_group = (
    tagged_2.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.prev_greatest_duration_sec_bpm") >= F.col("g.median_prev_greatest_duration_sec_bpm"))
    .select(
        "s.*",
        "g.median_prev_greatest_duration_sec_bpm",
        F.round(F.col("s.prev_greatest_duration_sec_bpm") - F.col("g.median_prev_greatest_duration_sec_bpm"), 2).alias("prev_greatest_duration_sec_bpm_minus_median"),
    )
)

result = (
    vs_group
    .filter(F.col("play_count") < 7)
    .select(
        "id",
        "is_explicit",
        "title_prefix",
        "greatest_duration_sec_bpm",
        "play_count",
        "bpm_avg_2_prior",
        "seq_no",
        "next2_bpm_filled",
        "seq_no_vs_artist_id_avg",
        "seq_no_change",
        "bpm_avg_2_prior_dense_rank",
        "play_count_per_next2_bpm_filled",
        "median_prev_greatest_duration_sec_bpm",
        (F.floor(F.col("next2_bpm_filled") / 25) * 25).alias("next2_bpm_filled_bucket"),
    )
)
