from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_release_date_year = Window.partitionBy(F.year("first_release_date"))
w_first_release_date_year_by_b = Window.partitionBy(F.year("first_release_date")).orderBy(F.col("bpm_bucket_to_days_since_release_date_ratio").desc(), "artist_country")
w_first_release_date_year_by_b_2 = Window.partitionBy(F.year("first_release_date")).orderBy(F.col("bpm_bucket_to_days_since_release_date_ratio").desc())

valid = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) | (F.lower(F.col("title")).like("% l%")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

with_details_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.moods",
        "s.days_since_release_date",
        "s.bpm_bucket",
        F.col("a.country").alias("artist_country"),
    )
)

summary = (
    with_details_artist
    .filter(F.col("days_since_release_date") < 595)
    .groupBy("artist_country")
    .agg(
        F.sum("bpm_bucket").alias("total_bpm_bucket"),
        F.round(F.sum("bpm_bucket") / F.nullif(F.sum("days_since_release_date"), F.lit(0)), 3).alias("bpm_bucket_to_days_since_release_date_ratio"),
        F.round(F.sum("days_since_release_date") / F.nullif(F.sum("bpm_bucket"), F.lit(0)), 3).alias("days_since_release_date_to_bpm_bucket_ratio"),
        F.min("release_date").alias("first_release_date"),
    )
)

sequenced = (
    summary
    .select(
        "artist_country",
        "bpm_bucket_to_days_since_release_date_ratio",
        F.size(F.collect_set("artist_country").over(w_first_release_date_year)).alias("distinct_artist_country_in_first_release_date_ye"),
        F.ntile(5).over(w_first_release_date_year_by_b).alias("bpm_bucket_to_days_since_release_date_ratio_quin"),
        F.lag("total_bpm_bucket", 2).over(w_first_release_date_year_by_b).alias("prev2_total_bpm_bucket"),
        F.rank().over(w_first_release_date_year_by_b_2).alias("bpm_bucket_to_days_since_release_date_ratio_rank"),
        F.rank().over(w_first_release_date_year_by_b).alias("bpm_bucket_to_days_since_release_date_ratio_rank_2"),
    )
)

result = (
    sequenced
    .groupBy("bpm_bucket_to_days_since_release_date_ratio_quin", "bpm_bucket_to_days_since_release_date_ratio_rank_2")
    .pivot("artist_country", ["BR", "US"])
    .agg(F.avg("distinct_artist_country_in_first_release_date_ye"))
)
