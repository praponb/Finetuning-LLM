from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy("release_date_week_week", "id", "mood")
w_all_by_bpm_ties = Window.orderBy("bpm")
w_mood = Window.partitionBy("mood")
w_all_by_bpm = Window.orderBy("bpm", "id", "mood")

cleaned = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

flattened = (
    cleaned
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "release_date_week",
        "title_ref",
        "title_len",
        F.explode_outer("moods").alias("mood"),
    )
)

enriched = (
    flattened
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "title_len",
        "mood",
        F.abs(F.col("title_len") - 7).alias("title_len_distance"),
        F.substring(F.trim(F.col("title_ref")), 1, 3).alias("title_ref_prefix"),
        F.date_trunc("week", F.col("release_date_week")).alias("release_date_week_week"),
    )
)

deduped = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

screened = (
    deduped.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") >= 100) & (~F.col("t.is_explicit")), "left_semi")
    .filter(F.col("artist_id") <= 2)
)

windowed = (
    screened
    .select(
        "id",
        "artist_id",
        "release_date",
        "mood",
        "title_len_distance",
        "title_ref_prefix",
        "release_date_week_week",
        F.round(F.percent_rank().over(w_all_by_bpm_ties), 3).alias("bpm_percent_rank"),
        F.size(F.collect_set("artist_id").over(w_mood)).alias("distinct_artist_id_in_mood"),
        F.count(F.lit(1)).over(w_mood).alias("rows_in_mood"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_bpm)).alias("days_since_prev_release_date"),
        F.lag("release_date_week_week").over(w_all_by_bpm).alias("prev_release_date_week_week"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "release_date",
        "mood",
        "title_len_distance",
        "title_ref_prefix",
        "release_date_week_week",
        "bpm_percent_rank",
        "distinct_artist_id_in_mood",
        "days_since_prev_release_date",
        "prev_release_date_week_week",
    )
    .orderBy(F.col("bpm_percent_rank").desc_nulls_first(), "id", "mood")
    .limit(10)
)
