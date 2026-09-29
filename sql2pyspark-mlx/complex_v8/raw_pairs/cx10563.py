from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_track_mood_track_id_by_debut = Window.partitionBy("track_mood", "track_id").orderBy("debut_year_distance", "id", "track_id", "track_mood")
w_all_by_debut_year_distance_f = Window.orderBy(F.col("debut_year_distance").desc_nulls_first(), "id", "track_id", "track_mood").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_debut_year_distance = Window.orderBy(F.col("debut_year_distance").desc_nulls_first(), "id", "track_id", "track_mood")
w_country_debut_year_distance = Window.partitionBy("country", "debut_year_distance_dense_rank").orderBy("track_id_min_debut_year_distance", "id", "track_id", "track_mood")
w_country_debut_year_distance_2 = Window.partitionBy("country", "debut_year_distance_dense_rank").orderBy("track_id_min_debut_year_distance", "id", "track_id", "track_mood").rowsBetween(-1, Window.currentRow)
w_track_is_explicit = Window.partitionBy("track_is_explicit")

valid = (
    df_artists
    .filter((F.col("country") == "TH") & (~F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

with_children_tracks = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.country",
        "s.debut_year",
        "s.is_verified",
        F.col("t.id").alias("track_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

flattened = (
    with_children_tracks
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        "track_id",
        "track_is_explicit",
        "track_release_date",
        F.explode("track_moods").alias("track_mood"),
    )
)

labeled = (
    flattened
    .select(
        "*",
        F.abs(F.col("debut_year") - 2015).alias("debut_year_distance"),
        F.when(F.col("country").isin("BR", "JP"), "group_a").when(F.col("country") == "TH", "group_b").otherwise("other").alias("country_group"),
        F.lower(F.regexp_replace(F.trim(F.col("track_mood")), " ", "_")).alias("track_mood_slug"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.min("debut_year_distance").over(w_track_id).alias("track_id_min_debut_year_distance"),
        F.dense_rank().over(w_track_mood_track_id_by_debut).alias("debut_year_distance_dense_rank"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.lead("track_release_date").over(w_track_mood_track_id_by_debut).alias("next_track_release_date"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "country",
        "debut_year",
        "track_id",
        "track_is_explicit",
        "track_release_date",
        "track_mood",
        "debut_year_distance",
        "track_mood_slug",
        "debut_year_distance_dense_rank",
        "rows_in_track_id",
        "next_track_release_date",
        F.nth_value("debut_year", 2).over(w_all_by_debut_year_distance_f).alias("second_debut_year"),
        F.round(F.percent_rank().over(w_all_by_debut_year_distance), 3).alias("debut_year_distance_percent_rank"),
        F.lag("debut_year", 2).over(w_country_debut_year_distance).alias("prev2_debut_year"),
        F.max("debut_year").over(w_country_debut_year_distance_2).alias("debut_year_max_2"),
        F.min("rows_in_track_id").over(w_track_is_explicit).alias("track_is_explicit_min_rows_in_track_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "track_id",
        "track_release_date",
        "debut_year_distance",
        "track_mood_slug",
        "debut_year_distance_dense_rank",
        "rows_in_track_id",
        "next_track_release_date",
        "second_debut_year",
        "debut_year_distance_percent_rank",
        "prev2_debut_year",
        "debut_year_max_2",
        "track_is_explicit_min_rows_in_track_id",
        F.coalesce(F.col("track_mood"), F.lit("UNKNOWN")).alias("track_mood_or_unknown"),
    )
    .orderBy(F.col("track_mood_slug").desc())
)
