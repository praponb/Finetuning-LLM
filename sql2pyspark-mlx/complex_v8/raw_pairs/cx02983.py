from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())
w_cnt_distinct_artist_countrys = Window.partitionBy("cnt", "distinct_artist_countrys")
w_distinct_artist_countrys_by = Window.partitionBy("distinct_artist_countrys").orderBy(F.col("distinct_artist_countrys").asc_nulls_last(), "artist_id", "release_date_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_cnt = Window.orderBy(F.col("cnt").desc_nulls_first(), "artist_id", "release_date_month")

filtered_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

latest = (
    filtered_tracks
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details = (
    latest.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

per_group = (
    with_details
    .filter(F.col("artist_genre").isNotNull())
    .groupBy(
        "artist_id",
        F.date_trunc("month", "release_date").alias("release_date_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.countDistinct("artist_country").alias("distinct_artist_countrys"),
    )
)

with_windows = (
    per_group
    .select(
        "artist_id",
        "release_date_month",
        "cnt",
        "release_date_span_days",
        "distinct_artist_countrys",
        F.round(F.col("cnt") - F.avg("cnt").over(w_cnt_distinct_artist_countrys), 2).alias("cnt_vs_cnt_distinct_artist_countrys_avg"),
        F.round(F.col("release_date_span_days") / F.nullif(F.sum("release_date_span_days").over(w_cnt_distinct_artist_countrys), F.lit(0)), 4).alias("release_date_span_days_share_of_cnt_distinct_art"),
        F.last("release_date_span_days", ignorenulls=True).over(w_distinct_artist_countrys_by).alias("last_known_release_date_span_days"),
        F.first("cnt", ignorenulls=True).over(w_distinct_artist_countrys_by).alias("first_known_cnt"),
        F.lag("distinct_artist_countrys", 1, 0).over(w_all_by_cnt).alias("prev_distinct_artist_countrys"),
    )
)

enriched_artist = (
    with_windows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.debut_year").alias("artist_debut_year"))
)

result = (
    enriched_artist
    .select(
        "artist_id",
        "release_date_month",
        "cnt",
        "release_date_span_days",
        "distinct_artist_countrys",
        "cnt_vs_cnt_distinct_artist_countrys_avg",
        "release_date_span_days_share_of_cnt_distinct_art",
        "last_known_release_date_span_days",
        "first_known_cnt",
        "prev_distinct_artist_countrys",
        "artist_debut_year",
    )
    .orderBy("last_known_release_date_span_days", "artist_id", "release_date_month")
    .limit(20)
)
