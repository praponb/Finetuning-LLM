from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_artist_country_artist_id_by_2 = Window.partitionBy("artist_country", "artist_id").orderBy(F.col("bpm").desc(), "id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_is_explicit_count_arti = Window.partitionBy("track_is_explicit_count", "artist_id").orderBy(F.col("bpm_quartile").asc_nulls_last(), "id")

valid_tracks = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2024-12-14").cast("date")) | (F.col("artist_id") <= 2))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
    )
)

latest_per_key = (
    valid_tracks
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

combined_details_artist = (
    latest_per_key.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.country").alias("artist_country"))
)

windowed = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        F.first("bpm").over(w_artist_country_artist_id_by).alias("first_bpm"),
        F.ntile(10).over(w_artist_country_artist_id_by_2).alias("bpm_decile"),
        F.ntile(4).over(w_artist_country_artist_id_by_2).alias("bpm_quartile"),
        F.sum("duration_sec").over(w_artist_id_artist_country).alias("artist_id_artist_country_sum_duration_sec"),
        F.nth_value("duration_sec", 2).over(w_all_by_bpm_full).alias("second_duration_sec"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_track_duration_sec"),
    )
)

summarized_rows = (
    windowed.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        "t.avg_track_duration_sec",
    )
)

sequenced = (
    summarized_rows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "first_bpm",
        "bpm_decile",
        "bpm_quartile",
        "artist_id_artist_country_sum_duration_sec",
        "track_is_explicit_count",
        F.lag("duration_sec", 2, 0).over(w_track_is_explicit_count_arti).alias("prev2_duration_sec"),
        F.lag("bpm").over(w_track_is_explicit_count_arti).alias("prev_bpm"),
        F.rank().over(w_track_is_explicit_count_arti).alias("bpm_quartile_rank"),
    )
)

combined_details_artist_2 = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.upper(F.trim(F.col("a.artist_name"))).like("L%"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.first_bpm",
        "s.bpm_decile",
        "s.bpm_quartile",
        "s.artist_id_artist_country_sum_duration_sec",
        "s.prev2_duration_sec",
        "s.bpm_quartile_rank",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    combined_details_artist_2
    .select(
        "id",
        "bpm_quartile",
        "bpm_decile",
        "artist_id_artist_country_sum_duration_sec",
        "duration_sec",
    )
    .unpivot(
        ids=["id"],
        values=["bpm_quartile", "bpm_decile", "artist_id_artist_country_sum_duration_sec", "duration_sec"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
