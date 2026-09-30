from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_by_releas = Window.partitionBy("artist_is_verified").orderBy(F.col("release_date").desc_nulls_first(), "id")
w_artist_country = Window.partitionBy("artist_country")
w_artist_country_by_release_da = Window.partitionBy("artist_country").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_by_releas_2 = Window.partitionBy("artist_is_verified").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_artist_is = Window.partitionBy(F.month("release_date"), "artist_is_verified").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_by_release_da_2 = Window.partitionBy("artist_country").orderBy("release_date", "id")

recent = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods",
        "s.release_date_label",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

windowed = (
    with_details
    .select(
        "id",
        "artist_id",
        "release_date",
        "moods",
        "release_date_label",
        "artist_country",
        "artist_debut_year",
        F.row_number().over(w_artist_is_verified_by_releas).alias("release_date_seq"),
        F.round(F.avg("artist_debut_year").over(w_artist_country), 2).alias("artist_country_avg_artist_debut_year"),
        F.count(F.lit(1)).over(w_artist_country).alias("rows_in_artist_country"),
        F.first("artist_debut_year", ignorenulls=True).over(w_artist_country_by_release_da).alias("first_known_artist_debut_year"),
        F.last("release_date").over(w_artist_is_verified_by_releas_2).alias("last_release_date"),
        F.count(F.lit(1)).over(w_release_date_month_artist_is).alias("running_count"),
        F.row_number().over(w_artist_country_by_release_da_2).alias("seq_no"),
        F.rank().over(w_artist_country_by_release_da_2).alias("release_date_rank"),
        F.count(F.lit(1)).over(w_artist_country_by_release_da).alias("running_count_2"),
    )
)

result = (
    windowed
    .filter(F.col("release_date_label").isNotNull())
    .select(
        "id",
        "artist_id",
        "release_date",
        "moods",
        "release_date_label",
        "artist_country",
        "release_date_seq",
        "artist_country_avg_artist_debut_year",
        "rows_in_artist_country",
        "first_known_artist_debut_year",
        "last_release_date",
        "running_count",
        "seq_no",
        "release_date_rank",
        "running_count_2",
        F.least(F.col("running_count"), F.col("release_date_rank")).alias("least_running_count_release_date_rank"),
        (F.col("release_date_seq") + F.col("seq_no")).alias("release_date_seq_plus_seq_no"),
    )
)
