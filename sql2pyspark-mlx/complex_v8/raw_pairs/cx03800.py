from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_mo = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")

recent_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 76) & (F.col("duration_sec") > 144))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
    )
)

with_trends = (
    recent_tracks
    .select(
        "*",
        F.sum("bpm_minus_duration_sec").over(w_artist_id_by_duration_sec_mo).alias("bpm_minus_duration_sec_sum_2"),
        F.row_number().over(w_artist_id_by_duration_sec).alias("seq_no"),
        F.round(F.col("bpm_minus_duration_sec") - F.avg("bpm_minus_duration_sec").over(w_artist_id), 2).alias("bpm_minus_duration_sec_vs_artist_id_avg"),
        F.sum("bpm_minus_duration_sec").over(w_artist_id).alias("artist_id_sum_bpm_minus_duration_sec"),
    )
)

joined_artist = (
    with_trends.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre").isin("Indie", "HipHop", "Pop"))
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    joined_artist
    .groupingSets(
        [["is_explicit_label", "artist_country"], ["is_explicit_label"], []],
        "is_explicit_label", "artist_country",
    )
    .agg(
        F.grouping("is_explicit_label").alias("is_total_is_explicit_label"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("is_explicit_label"), F.lit("ALL")).alias("is_explicit_label"),
        F.coalesce(F.col("artist_country"), F.lit("ALL")).alias("artist_country"),
        "is_total_is_explicit_label",
        "row_count",
    )
    .orderBy(
        F.col("is_explicit_label").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)
