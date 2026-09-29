from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())

scoped = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

unique_rows = (
    scoped
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    unique_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isNotNull()) & (F.col("p.ms_played") >= 107912), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["is_explicit", "artist_id"], ["is_explicit"], ["artist_id"], []],
        "is_explicit", "artist_id",
    )
    .agg(
        F.grouping("is_explicit").alias("is_total_is_explicit"),
        F.sum("bpm").alias("total_bpm"),
        F.max("release_date").alias("last_release_date"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
