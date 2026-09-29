from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_by_durati = Window.partitionBy("artist_is_verified").orderBy("duration_sec", "id")
w_artist_is_verified_release_d = Window.partitionBy("artist_is_verified", F.year("release_date"))
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_is_verified_2 = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("release_date").desc())

filtered = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

enriched_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isin("JP", "TH")), "inner")
    .filter(~F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.moods_text",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

windowed = (
    enriched_artist
    .select(
        "*",
        F.ntile(10).over(w_artist_is_verified_by_durati).alias("duration_sec_decile"),
        F.count(F.lit(1)).over(w_artist_is_verified_release_d).alias("rows_in_artist_is_verified_release_date_year"),
        F.max("duration_sec").over(w_artist_is_verified_artist_id).alias("max_duration_sec_so_far"),
        F.max("duration_sec").over(w_artist_is_verified_release_d).alias("artist_is_verified_release_date_year_max_duratio"),
        F.size(F.collect_set("artist_id").over(w_artist_is_verified_release_d)).alias("distinct_artist_id_in_artist_is_verified_release"),
        F.first("duration_sec", ignorenulls=True).over(w_artist_id_artist_is_verified).alias("first_known_duration_sec"),
        F.round(F.percent_rank().over(w_artist_id_artist_is_verified_2), 3).alias("release_date_percent_rank"),
        F.round(F.avg("duration_sec").over(w_artist_is_verified_release_d), 2).alias("artist_is_verified_release_date_year_avg_duratio"),
    )
)

enriched = (
    windowed
    .select(
        "*",
        F.abs(F.col("first_known_duration_sec") - 184).alias("first_known_duration_sec_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("moods_text")), " ", "_")).alias("moods_text_slug"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

result = (
    enriched
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "moods",
        "moods_text",
        "artist_is_verified",
        "duration_sec_decile",
        "rows_in_artist_is_verified_release_date_year",
        "max_duration_sec_so_far",
        "artist_is_verified_release_date_year_max_duratio",
        "distinct_artist_id_in_artist_is_verified_release",
        "first_known_duration_sec",
        "release_date_percent_rank",
        "artist_is_verified_release_date_year_avg_duratio",
        "first_known_duration_sec_distance",
        "moods_text_slug",
        "release_date_month",
        F.date_format(F.col("release_date_month"), "MMM yyyy").alias("release_date_month_label"),
    )
    .orderBy(F.col("release_date_month_label").desc())
)
