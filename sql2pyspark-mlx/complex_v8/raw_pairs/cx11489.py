from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id")
w_release_date_week_month_by_b = Window.partitionBy(F.date_trunc("month", "release_date_week")).orderBy("bpm", "id")

base_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("N%")) & (F.col("duration_sec") > 165))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.round(F.avg("bpm"), 2).alias("avg_track_bpm"),
        F.min("release_date").alias("first_track_release_date"),
        F.count("*").alias("track_count"),
    )
)

with_stats = (
    base_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.avg_track_bpm",
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

with_details_artist = (
    with_stats.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.release_date_week",
        "s.title_clean",
        "s.first_track_release_date",
        "s.track_count",
        F.col("a.genre").alias("artist_genre"),
    )
)

sequenced = (
    with_details_artist
    .select(
        "*",
        F.dense_rank().over(w_all_by_duration_sec).alias("duration_sec_dense_rank"),
        F.count(F.lit(1)).over(w_release_date_year_artist_id).alias("rows_in_release_date_year_artist_id"),
        F.round(F.avg("duration_sec").over(w_release_date_year_artist_id), 2).alias("release_date_year_artist_id_avg_duration_sec"),
        F.size(F.collect_set("artist_genre").over(w_release_date_year_artist_id)).alias("distinct_artist_genre_in_release_date_year_artis"),
        F.row_number().over(w_release_date_week_month_by_b).alias("bpm_seq"),
    )
)

result = (
    sequenced
    .select("id", "bpm_seq", "duration_sec_dense_rank")
    .unpivot(
        ids=["id"],
        values=["bpm_seq", "duration_sec_dense_rank"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
