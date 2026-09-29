from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_label_2_is_expli = Window.partitionBy("is_explicit_label_2", "is_explicit_label")
w_all_by_bpm = Window.orderBy("bpm", "id")

valid_tracks = (
    df_tracks
    .filter((F.year(F.col("release_date")) == 2024) | (F.col("duration_sec") > 209))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

enriched = (
    valid_tracks
    .select(
        "*",
        F.concat_ws("|", F.col("title_slug"), F.col("is_explicit_label")).alias("title_slug_is_explicit_label_label"),
        F.concat(F.col("title_slug"), F.lit("#"), F.col("bpm").cast("string")).alias("title_slug_ref"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label_2"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_explicit_label_2_is_expli).alias("rows_in_is_explicit_label_2_is_explicit_label"),
        F.round(F.percent_rank().over(w_all_by_bpm), 3).alias("bpm_percent_rank"),
    )
)

result = (
    with_trends
    .groupBy("artist_id", "is_explicit")
    .pivot("is_explicit_label", ["yes", "no"])
    .agg(F.sum("duration_sec"))
)
