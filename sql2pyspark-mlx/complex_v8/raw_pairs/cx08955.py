from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_first_played_at = Window.partitionBy("artist_id").orderBy("first_played_at", "id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc_nulls_first(), "id")
w_artist_id_by_bpm_prior_6 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_played_at_month = Window.partitionBy(F.month("first_played_at"))
w_first_played_at_month_rn_by = Window.partitionBy(F.date_trunc("month", "first_played_at"), "rn").orderBy(F.col("artist_id_avg_bpm").desc(), "id")

valid = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%in%"))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

summarized_rows = (
    valid.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at")
)

ranked = (
    summarized_rows
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        "first_played_at",
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.round((F.unix_timestamp("first_played_at") - F.unix_timestamp(F.lag("first_played_at").over(w_artist_id_by_first_played_at))) / 60, 1).alias("minutes_since_prev_first_played_at"),
        F.row_number().over(w_artist_id_by_bpm).alias("rn"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_first_played_at)).alias("bpm_change"),
        F.max("bpm").over(w_artist_id_by_bpm_prior_6).alias("bpm_max_6_prior"),
    )
)

combined_details_artist = (
    ranked.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.debut_year").alias("artist_debut_year"), "a.artist_name")
)

with_trends = (
    combined_details_artist
    .select(
        "*",
        F.first("artist_debut_year", ignorenulls=True).over(w_artist_id_by_bpm_running).alias("first_known_artist_debut_year"),
        F.max("bpm_change").over(w_first_played_at_month).alias("first_played_at_month_max_bpm_change"),
        F.round(F.percent_rank().over(w_first_played_at_month_rn_by), 3).alias("artist_id_avg_bpm_percent_rank"),
    )
)

derived = (
    with_trends
    .withColumn("first_played_at_label", F.date_format(F.col("first_played_at"), "yyyy-MM-dd"))
)

result = (
    derived
    .cube("artist_id", "is_explicit")
    .agg(F.count("*").alias("cnt"))
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
