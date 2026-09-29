from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy("moods_count", "id")

prepared = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 6) & (F.array_contains(F.col("moods"), "chill")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug_2"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

labeled = (
    prepared
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        "title_slug",
        "title_slug_2",
        "is_explicit_label",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

filtered = (
    labeled.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.is_skipped")) & (F.col("p.ms_played") > 42041), "left_anti")
    .filter(F.length(F.trim(F.col("title"))) >= 6)
)

unique_rows = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .rollup("artist_id", "moods_count", "is_explicit")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("moods_count").alias("total_moods_count"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("moods_count").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
