from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len = Window.partitionBy("title_len")
w_all_by_moods_count = Window.orderBy("moods_count", "id")

cleaned = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("P%")) & (F.col("artist_id") <= 6))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_title_len)).alias("distinct_artist_id_in_title_len"),
        F.ntile(5).over(w_all_by_moods_count).alias("moods_count_quintile"),
        F.round(F.col("title_len") - F.avg("title_len").over(w_title_len), 2).alias("title_len_vs_title_len_avg"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_title_len), 2).alias("bpm_vs_title_len_avg"),
    )
)

enriched = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.moods_count",
        "s.title_len",
        "s.release_date_label",
        "s.distinct_artist_id_in_title_len",
        "s.moods_count_quintile",
        "s.title_len_vs_title_len_avg",
        "s.bpm_vs_title_len_avg",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

enriched_2 = (
    enriched
    .select(
        "*",
        (F.col("bpm_vs_title_len_avg") - F.col("moods_count")).alias("bpm_vs_title_len_avg_minus_moods_count"),
        F.length(F.trim(F.col("release_date_label"))).alias("release_date_label_len"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

enriched_artist = (
    enriched_2.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.moods_count",
        "s.release_date_label",
        "s.distinct_artist_id_in_title_len",
        "s.moods_count_quintile",
        "s.artist_is_verified",
        "s.bpm_vs_title_len_avg_minus_moods_count",
        "s.release_date_label_len",
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

result = (
    enriched_artist
    .select(
        "id",
        "release_date_label_len",
        "moods_count_quintile",
        "moods_count",
        "duration_sec",
    )
    .unpivot(
        ids=["id"],
        values=["release_date_label_len", "moods_count_quintile", "moods_count", "duration_sec"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
