from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("distinct_artist_ids_tier").orderBy(F.col("release_date_ym_len").desc(), F.col("release_date_ym").desc(), F.col("artist_id").desc())

filtered = (
    df_tracks
    .filter((F.col("artist_id") >= 12) & (F.col("bpm") <= 155))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.date_add(F.col("release_date"), 365).alias("release_date_plus_365d"),
    )
)

artist_id_stats = (
    filtered
    .groupBy("artist_id")
    .agg(F.max("bpm").alias("max_bpm"))
)

vs_group = (
    filtered.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") == F.col("g.max_bpm"))
    .select(
        "s.*",
        "g.max_bpm",
        F.round(F.col("s.bpm") - F.col("g.max_bpm"), 2).alias("bpm_minus_max"),
    )
)

enriched = (
    vs_group.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.release_date_plus_365d",
        "s.max_bpm",
        "s.bpm_minus_max",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
    )
)

rollup_base = (
    enriched
    .groupBy(
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
        "artist_id",
    )
    .agg(
        F.round(F.stddev("max_bpm"), 2).alias("stddev_max_bpm"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.max("duration_sec").alias("max_duration_sec_check"),
    )
    .filter(F.col("max_duration_sec_check") > 164)
    .drop("max_duration_sec_check")
)

scored = (
    rollup_base
    .select(
        "*",
        F.when(F.col("distinct_artist_ids") >= 7, "large").when(F.col("distinct_artist_ids") >= 5, "medium").otherwise("small").alias("distinct_artist_ids_tier"),
        F.concat(F.col("release_date_ym"), F.lit("#"), F.col("distinct_artist_ids").cast("string")).alias("release_date_ym_ref"),
        F.length(F.trim(F.col("release_date_ym"))).alias("release_date_ym_len"),
    )
)

unique_rows = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .filter(F.col("distinct_artist_ids_tier") == "large")
    .select("release_date_ym", "distinct_artist_ids", "distinct_artist_ids_tier")
)
