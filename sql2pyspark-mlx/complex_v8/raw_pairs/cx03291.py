from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("is_explicit_label").orderBy(F.col("bpm").desc(), F.col("id").desc())

base = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

tagged = (
    deduped
    .filter(F.col("artist_id") >= 11)
    .withColumn("duration_sec_filled", F.coalesce(F.col("duration_sec"), F.lit(0)))
)

annotated = (
    tagged
    .select(
        "*",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled_2"),
    )
)

result = (
    annotated
    .groupingSets(
        [["artist_id", "is_explicit_label"], ["artist_id"]],
        "artist_id", "is_explicit_label",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("bpm").alias("min_bpm"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit_label").asc_nulls_last(),
    )
)
