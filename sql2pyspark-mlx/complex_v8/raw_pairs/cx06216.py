from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_moods_count = Window.partitionBy("artist_id", "moods_count")
w_all_by_title_len_prior_6 = Window.orderBy("title_len", "id").rowsBetween(-6, -1)
w_all_by_title_len = Window.orderBy("title_len", "id")

cleaned_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "focus")) & (F.col("release_date") < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

sequenced = (
    cleaned_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "title_len",
        "moods_count",
        F.min("bpm").over(w_artist_id_moods_count).alias("artist_id_moods_count_min_bpm"),
        F.round(F.avg("title_len").over(w_all_by_title_len_prior_6), 2).alias("title_len_avg_6_prior"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_moods_count), 2).alias("bpm_vs_artist_id_moods_count_avg"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_title_len)).alias("days_since_prev_release_date"),
    )
)

result = (
    sequenced
    .select(
        "id",
        F.col("title_len_avg_6_prior").alias("title_len_avg_6_prior_v"),
        F.col("bpm_vs_artist_id_moods_count_avg").alias("m_bpm_vs_artist_id_moods_count_avg"),
    )
    .unpivot(
        ids=["id"],
        values=["title_len_avg_6_prior_v", "m_bpm_vs_artist_id_moods_count_avg"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
