from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_artist_id_artist_country_by = Window.partitionBy("artist_id", "artist_country").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy("bpm")
w_artist_country_by_bpm_ties = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc())
w_artist_country_by_bpm = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc(), "id")
w_artist_id_artist_country_by_2 = Window.partitionBy("artist_id", "artist_country").orderBy(F.col("bpm").desc(), "id")
w_artist_country_bpm_decile = Window.partitionBy("artist_country", "bpm_decile")
w_bpm_quartile_by_bpm_change_f = Window.partitionBy("bpm_quartile").orderBy(F.col("bpm_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_bpm_quartile_by_bpm_percent = Window.partitionBy("bpm_quartile").orderBy(F.col("bpm_percent_rank").desc(), "id")

valid = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
    )
)

with_details_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "Pop"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

windowed = (
    with_details_artist
    .select(
        "*",
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.round(F.percent_rank().over(w_all_by_bpm), 3).alias("bpm_percent_rank"),
        F.nth_value("bpm", 2).over(w_artist_id_artist_country_by).alias("second_bpm"),
        F.dense_rank().over(w_all_by_bpm).alias("bpm_dense_rank"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.cume_dist().over(w_artist_country_artist_id_by), 3).alias("bpm_cume_dist"),
        F.round(F.percent_rank().over(w_artist_country_artist_id_by), 3).alias("bpm_percent_rank_2"),
        F.dense_rank().over(w_artist_country_by_bpm_ties).alias("bpm_dense_rank_2"),
        F.ntile(10).over(w_all_by_bpm).alias("bpm_decile"),
        F.ntile(5).over(w_artist_country_by_bpm).alias("bpm_quintile"),
        F.ntile(4).over(w_artist_id_artist_country_by_2).alias("bpm_quartile"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_bpm)).alias("bpm_change"),
    )
)

top_per_group = (
    windowed
    .filter((F.col("bpm_dense_rank_2") <= 3) & (F.col("bpm_decile") > 4))
    .drop("bpm_dense_rank_2")
)

ranked = (
    top_per_group
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_artist_country_bpm_decile)).alias("distinct_artist_id_in_artist_country_bpm_decile"),
        F.nth_value("bpm_dense_rank", 2).over(w_bpm_quartile_by_bpm_change_f).alias("second_bpm_dense_rank"),
        F.count(F.lit(1)).over(w_artist_country_bpm_decile).alias("rows_in_artist_country_bpm_decile"),
        (F.col("bpm_percent_rank") - F.lag("bpm_percent_rank").over(w_bpm_quartile_by_bpm_percent)).alias("bpm_percent_rank_change"),
        F.lag("bpm", 3).over(w_bpm_quartile_by_bpm_percent).alias("prev3_bpm"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "artist_name",
        "artist_country",
        "artist_id_avg_bpm",
        "bpm_percent_rank",
        "second_bpm",
        "bpm_dense_rank",
        "rows_in_artist_id",
        "bpm_cume_dist",
        "bpm_percent_rank_2",
        "bpm_quintile",
        "distinct_artist_id_in_artist_country_bpm_decile",
        "rows_in_artist_country_bpm_decile",
        "prev3_bpm",
    )
)
