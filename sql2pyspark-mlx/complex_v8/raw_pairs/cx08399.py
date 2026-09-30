from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_label_by_debut_y = Window.partitionBy("is_verified_label").orderBy("debut_year_bucket_2", "id").rowsBetween(-1, Window.currentRow)
w_is_verified_label_by_debut_y_2 = Window.partitionBy("is_verified_label").orderBy("debut_year_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_verified_label_by_debut_y_3 = Window.partitionBy("is_verified_label").orderBy("debut_year_bucket_2", "id")
w_is_verified_label = Window.partitionBy("is_verified_label")

recent_artists = (
    df_artists
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "is_verified",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket_2"),
    )
)

labeled = (
    recent_artists
    .select(
        "id",
        "artist_name",
        "debut_year_bucket",
        "debut_year_bucket_2",
        (F.col("debut_year") + F.col("debut_year_bucket")).alias("debut_year_plus_debut_year_bucket"),
        F.when(F.col("is_verified"), "yes").otherwise("no").alias("is_verified_label"),
        (F.col("debut_year") + F.col("debut_year_bucket_2")).alias("debut_year_plus_debut_year_bucket_2"),
    )
)

metrics = (
    labeled
    .select(
        "*",
        F.max("debut_year_bucket_2").over(w_is_verified_label_by_debut_y).alias("debut_year_bucket_2_max_2"),
        F.first("debut_year_bucket_2", ignorenulls=True).over(w_is_verified_label_by_debut_y_2).alias("first_known_debut_year_bucket_2"),
        F.row_number().over(w_is_verified_label_by_debut_y_3).alias("seq_no"),
        F.round(F.col("debut_year_bucket") - F.avg("debut_year_bucket").over(w_is_verified_label), 2).alias("debut_year_bucket_vs_is_verified_label_avg"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (~F.array_contains(F.col("t.moods"), "sad")), "left_anti")
)

labeled_2 = (
    filtered
    .select(
        "id",
        "debut_year_bucket",
        "debut_year_plus_debut_year_bucket",
        "debut_year_plus_debut_year_bucket_2",
        "debut_year_bucket_2_max_2",
        "first_known_debut_year_bucket_2",
        "debut_year_bucket_vs_is_verified_label_avg",
        F.round(F.col("debut_year_bucket_2") / F.nullif(F.col("debut_year_bucket"), F.lit(0)), 3).alias("debut_year_bucket_2_per_debut_year_bucket"),
        F.upper(F.trim(F.col("is_verified_label"))).alias("is_verified_label_clean"),
    )
)

result = (
    labeled_2
    .filter(F.length(F.trim(F.col("is_verified_label_clean"))) >= 5)
    .select(
        "id",
        "debut_year_bucket",
        "debut_year_plus_debut_year_bucket",
        "debut_year_plus_debut_year_bucket_2",
        "debut_year_bucket_2_max_2",
        "debut_year_bucket_2_per_debut_year_bucket",
        F.substring(F.trim(F.col("is_verified_label_clean")), 1, 4).alias("is_verified_label_clean_prefix"),
        F.substring(F.trim(F.col("is_verified_label_clean")), 1, 3).alias("is_verified_label_clean_prefix_2"),
    )
    .orderBy(F.col("debut_year_plus_debut_year_bucket_2").asc_nulls_last(), "id")
    .limit(5)
)
