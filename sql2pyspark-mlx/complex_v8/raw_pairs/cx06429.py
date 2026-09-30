from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_by_least_bpm_dur = Window.partitionBy("is_explicit").orderBy("least_bpm_duration_sec", "id").rowsBetween(-2, -1)
w_artist_id_by_bpm_minus_durat = Window.partitionBy("artist_id").orderBy("bpm_minus_duration_sec")
w_artist_id_by_bpm_minus_durat_2 = Window.partitionBy("artist_id").orderBy("bpm_minus_duration_sec", "id")
w_is_explicit_by_least_bpm_dur_2 = Window.partitionBy("is_explicit").orderBy("least_bpm_duration_sec")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id").orderBy("bpm", "id")

recent_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("G%"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

scored = (
    recent_tracks
    .select(
        "*",
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.round(F.col("least_bpm_duration_sec") / F.nullif(F.sum("least_bpm_duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("least_bpm_duration_sec_share_of_artist_id_is_exp"),
        F.max("bpm").over(w_is_explicit_by_least_bpm_dur).alias("bpm_max_2_prior"),
        F.rank().over(w_artist_id_by_bpm_minus_durat).alias("bpm_minus_duration_sec_rank"),
        F.lead("release_date_week").over(w_artist_id_by_bpm_minus_durat_2).alias("next_release_date_week"),
        F.round(F.percent_rank().over(w_is_explicit_by_least_bpm_dur_2), 3).alias("least_bpm_duration_sec_percent_rank"),
        F.dense_rank().over(w_release_date_month_artist_id).alias("bpm_dense_rank"),
    )
)

top_rows = with_windows.filter(F.col("bpm_dense_rank") == 1)

enriched = (
    top_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.release_date_week",
        "s.bpm_filled",
        "s.bpm_minus_duration_sec",
        "s.release_date_label",
        "s.least_bpm_duration_sec",
        "s.least_bpm_duration_sec_share_of_artist_id_is_exp",
        "s.next_release_date_week",
        "s.least_bpm_duration_sec_percent_rank",
        "s.bpm_dense_rank",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

enriched_2 = (
    enriched
    .select(
        "id",
        "is_explicit",
        "release_date_week",
        "bpm_filled",
        "bpm_minus_duration_sec",
        "release_date_label",
        "least_bpm_duration_sec",
        "least_bpm_duration_sec_share_of_artist_id_is_exp",
        "next_release_date_week",
        "bpm_dense_rank",
        "artist_name",
        "artist_is_verified",
        "artist_genre",
        F.length(F.trim(F.col("artist_name"))).alias("artist_name_len"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm_minus_duration_sec"), F.lit(0)), 3).alias("duration_sec_per_bpm_minus_duration_sec"),
        F.to_date(F.col("release_date_week")).alias("release_date_week_date"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "duration_sec_per_bpm_minus_duration_sec",
        "least_bpm_duration_sec_share_of_artist_id_is_exp",
    )
    .unpivot(
        ids=["id"],
        values=["duration_sec_per_bpm_minus_duration_sec", "least_bpm_duration_sec_share_of_artist_id_is_exp"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
