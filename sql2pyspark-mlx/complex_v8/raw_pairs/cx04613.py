from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_track_id_by_row_co = Window.partitionBy("artist_id", "track_id").orderBy(F.col("row_count").desc(), "release_date_month", "artist_id", "track_id", "title_list_item").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count = Window.partitionBy("row_count")

base_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 94) & (F.col("release_date") < F.lit("2025-08-02").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

tagged = (
    base_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

filtered = (
    tagged.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("M%")) & (F.col("t.duration_sec") <= 365), "left_semi")
)

summary = (
    filtered
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month"),
        "artist_id",
    )
    .agg(
        F.min("days_since_release_date").alias("min_days_since_release_date"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("E%"), F.col("bpm")).otherwise(0)).alias("bpm_upper_trim_title_like_e"),
        F.max("release_date").alias("last_release_date"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
    )
)

with_children_tracks = (
    summary.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (~F.col("t.is_explicit")), "inner")
    .select(
        "s.release_date_month",
        "s.artist_id",
        "s.row_count",
        "s.last_release_date",
        "s.title_list",
        F.col("t.id").alias("track_id"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

per_label = (
    with_children_tracks
    .select(
        "release_date_month",
        "artist_id",
        "row_count",
        "last_release_date",
        "track_id",
        "track_moods",
        "track_is_explicit",
        F.explode_outer("title_list").alias("title_list_item"),
    )
)

with_trends = (
    per_label
    .select(
        "*",
        F.last("last_release_date", ignorenulls=True).over(w_artist_id_track_id_by_row_co).alias("last_known_last_release_date"),
        F.round(F.col("row_count") - F.avg("row_count").over(w_row_count), 2).alias("row_count_vs_row_count_avg"),
    )
)

result = (
    with_trends
    .filter(F.year(F.col("last_release_date")) == 2024)
    .select(
        "release_date_month",
        "artist_id",
        "row_count",
        "last_release_date",
        "track_id",
        "track_moods",
        "track_is_explicit",
        "title_list_item",
        "last_known_last_release_date",
        "row_count_vs_row_count_avg",
        F.when(F.col("row_count") >= 9, "HIGH").when(F.col("row_count") >= 5, "MEDIUM").otherwise("LOW").alias("row_count_tier"),
    )
    .orderBy(F.col("title_list_item").desc())
)
