from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_distinct_artist = Window.partitionBy("is_explicit", "distinct_artist_ids")
w_artist_country_by_total_dura = Window.partitionBy("artist_country").orderBy("total_duration_sec", "is_explicit", "artist_id", "release_date_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_by_total_dura_2 = Window.partitionBy("artist_country").orderBy("total_duration_sec")

cleaned = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-09-26").cast("date"), F.lit("2025-08-02").cast("date"))) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

with_parent_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.title_ref",
        "s.release_date_label",
        "s.least_bpm_duration_sec",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

rollup_base = (
    with_parent_artist
    .groupBy(
        "is_explicit",
        "artist_id",
        F.date_trunc("month", "release_date").alias("release_date_month"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.sum("duration_sec") / F.nullif(F.sum("least_bpm_duration_sec"), F.lit(0)), 3).alias("duration_sec_to_least_bpm_duration_sec_ratio"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
)

labeled = (
    rollup_base
    .select(
        "is_explicit",
        "artist_id",
        "release_date_month",
        "total_duration_sec",
        "distinct_artist_ids",
        F.year(F.col("release_date_month")).alias("release_date_month_year"),
    )
)

joined = (
    labeled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.is_explicit",
        "s.artist_id",
        "s.release_date_month",
        "s.total_duration_sec",
        "s.distinct_artist_ids",
        F.col("a.country").alias("artist_country"),
    )
)

windowed = (
    joined
    .select(
        "is_explicit",
        "artist_id",
        "release_date_month",
        "artist_country",
        F.size(F.collect_set("artist_id").over(w_is_explicit_distinct_artist)).alias("distinct_artist_id_in_is_explicit_distinct_artis"),
        F.min("distinct_artist_ids").over(w_artist_country_by_total_dura).alias("min_distinct_artist_ids_so_far"),
        F.rank().over(w_artist_country_by_total_dura_2).alias("total_duration_sec_rank"),
        F.min("distinct_artist_ids").over(w_is_explicit_distinct_artist).alias("is_explicit_distinct_artist_ids_min_distinct_art"),
    )
)

with_details_artist = (
    windowed.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.is_explicit",
        "s.artist_id",
        "s.release_date_month",
        "s.distinct_artist_id_in_is_explicit_distinct_artis",
        "s.min_distinct_artist_ids_so_far",
        "s.total_duration_sec_rank",
        "s.is_explicit_distinct_artist_ids_min_distinct_art",
        F.col("a.country").alias("artist_country_2"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    with_details_artist
    .groupBy("distinct_artist_id_in_is_explicit_distinct_artis")
    .pivot("artist_country_2", ["US", "JP", "BR"])
    .agg(F.max("total_duration_sec_rank"))
    .orderBy("distinct_artist_id_in_is_explicit_distinct_artis")
)
