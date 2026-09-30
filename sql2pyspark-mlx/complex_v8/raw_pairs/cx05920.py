from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit").orderBy("duration_sec", "id")
w_all_by_duration_sec_rank = Window.orderBy("duration_sec_rank", "id")
w_duration_sec_rank_artist_id = Window.partitionBy("duration_sec_rank", "artist_id")
w_all_by_duration_sec_rank_ful = Window.orderBy("duration_sec_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_rank_tie = Window.orderBy("duration_sec_rank")

recent_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("G%")) | (F.col("duration_sec") > 213))
)

with_windows = (
    recent_tracks
    .select(
        "*",
        F.rank().over(w_is_explicit_by_duration_sec).alias("duration_sec_rank"),
        F.first("duration_sec", ignorenulls=True).over(w_is_explicit_by_duration_sec_2).alias("first_known_duration_sec"),
        F.row_number().over(w_release_date_month_is_explic).alias("seq_no"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit_artist_id), 2).alias("bpm_vs_is_explicit_artist_id_avg"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_rank",
        "seq_no",
        F.lead("bpm").over(w_all_by_duration_sec_rank).alias("next_bpm"),
        F.round(F.col("first_known_duration_sec") / F.nullif(F.sum("first_known_duration_sec").over(w_duration_sec_rank_artist_id), F.lit(0)), 4).alias("first_known_duration_sec_share_of_duration_sec_r"),
        F.round(F.col("duration_sec_rank") / F.nullif(F.sum("duration_sec_rank").over(w_duration_sec_rank_artist_id), F.lit(0)), 4).alias("duration_sec_rank_share_of_duration_sec_rank_art"),
        F.last("seq_no").over(w_all_by_duration_sec_rank_ful).alias("last_seq_no"),
        F.sum("first_known_duration_sec").over(w_duration_sec_rank_artist_id).alias("duration_sec_rank_artist_id_sum_first_known_dura"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_rank_tie), 3).alias("duration_sec_rank_cume_dist"),
    )
)

filtered = (
    with_windows_2
    .crossJoin(with_windows_2.agg(F.avg("last_seq_no").alias("overall_avg_last_seq_no")))
    .filter(F.col("last_seq_no") > F.col("overall_avg_last_seq_no"))
    .drop("overall_avg_last_seq_no")
)

labeled = (
    filtered
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

without_matches = (
    labeled.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("C%")) & (F.size(F.col("t.moods")) >= 2), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_rank_share_of_duration_sec_rank_art",
        "last_seq_no",
        "duration_sec_rank_cume_dist",
        "title_slug",
        F.coalesce(F.col("next_bpm"), F.lit(0)).alias("next_bpm_filled"),
    )
)
