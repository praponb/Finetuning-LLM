from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy("release_date")
w_latest = Window.partitionBy("play_source").orderBy(F.col("play_played_at").desc(), F.col("id").desc(), F.col("play_id").desc())

prepared = (
    df_tracks
    .filter((F.col("duration_sec") > 165) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix_2"),
    )
)

with_trends = (
    prepared
    .withColumn("second_release_date", F.nth_value("release_date", 2).over(w_artist_id_by_release_date_fu))
    .withColumn("release_date_percent_rank", F.round(F.percent_rank().over(w_artist_id_by_release_date_ti), 3))
)

detail_rows_plays = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.title_prefix_2",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
    )
)

annotated = (
    detail_rows_plays
    .withColumn("title_title_prefix_2_label", F.concat_ws(": ", F.col("title"), F.col("title_prefix_2")))
)

deduped = (
    annotated
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .filter(F.col("artist_id") >= 9)
    .select(
        "id",
        "title_prefix_2",
        "play_device",
        "play_played_at",
        "play_source",
        "title_title_prefix_2_label",
        F.coalesce(F.col("play_device"), F.lit("UNKNOWN")).alias("play_device_or_unknown"),
        F.when(F.col("play_device").isin("phone", "desktop"), "group_a").when(F.col("play_device") == "car", "group_b").otherwise("other").alias("play_device_group"),
    )
)
