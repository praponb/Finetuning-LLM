from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_artist_genre = Window.partitionBy("artist_id", "artist_genre")
w_artist_id_artist_genre_by_du = Window.partitionBy("artist_id", "artist_genre").orderBy("duration_sec", "id")
w_artist_country = Window.partitionBy("artist_country")
w_artist_id_artist_genre_by_du_2 = Window.partitionBy("artist_id", "artist_genre").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_artist_genre = Window.partitionBy("artist_country", "artist_genre")
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")

scoped = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.year(F.col("release_date")) == 2025))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "Jazz"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.nth_value("duration_sec", 2).over(w_all_by_duration_sec_full).alias("second_duration_sec"),
        F.min("duration_sec").over(w_artist_id_artist_genre).alias("artist_id_artist_genre_min_duration_sec"),
        F.round(F.cume_dist().over(w_artist_id_artist_genre_by_du), 3).alias("duration_sec_cume_dist"),
        F.max("duration_sec").over(w_artist_id_artist_genre).alias("artist_id_artist_genre_max_duration_sec"),
        F.size(F.collect_set("artist_country").over(w_artist_id_artist_genre)).alias("distinct_artist_country_in_artist_id_artist_genr"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_country), F.lit(0)), 4).alias("duration_sec_share_of_artist_country"),
        F.last("duration_sec", ignorenulls=True).over(w_artist_id_artist_genre_by_du_2).alias("last_known_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_country_artist_genre), F.lit(0)), 4).alias("duration_sec_share_of_artist_country_artist_genr"),
        F.size(F.collect_set("artist_id").over(w_artist_country)).alias("distinct_artist_id_in_artist_country"),
        F.count(F.lit(1)).over(w_artist_id_artist_genre).alias("rows_in_artist_id_artist_genre"),
        F.rank().over(w_all_by_duration_sec_ties).alias("duration_sec_rank"),
        F.lag("duration_sec", 2).over(w_all_by_duration_sec).alias("prev2_duration_sec"),
        F.ntile(10).over(w_all_by_duration_sec).alias("duration_sec_decile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "artist_genre",
        "artist_country",
        "second_duration_sec",
        "artist_id_artist_genre_min_duration_sec",
        "last_known_duration_sec",
        "distinct_artist_id_in_artist_country",
        "duration_sec_rank",
        "prev2_duration_sec",
        "duration_sec_decile",
    )
    .orderBy(
        "artist_id_artist_genre_min_duration_sec",
        F.col("prev2_duration_sec").desc(),
        "id",
    )
    .limit(10)
)
