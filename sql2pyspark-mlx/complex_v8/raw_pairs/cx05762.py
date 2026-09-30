from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter(F.col("bpm") <= 143)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
    )
)

tagged = (
    prepared_tracks
    .filter(F.col("bpm_minus_duration_sec") >= 168)
    .select(
        "*",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.initcap(F.trim(F.col("title_ref"))).alias("title_ref_clean"),
    )
)

qualified = (
    tagged
    .crossJoin(tagged.agg(F.avg("bpm").alias("overall_avg_bpm")))
    .filter(F.col("bpm") > F.col("overall_avg_bpm"))
    .drop("overall_avg_bpm")
)

exploded = (
    qualified
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "title_ref",
        "bpm_minus_duration_sec",
        "release_date_month_start",
        "title_len",
        "title_ref_clean",
        F.posexplode("moods").alias("mood_pos", "mood"),
    )
)

mood_stats = (
    exploded
    .groupBy("mood")
    .agg(F.percentile_approx("bpm_minus_duration_sec", 0.5).alias("median_bpm_minus_duration_sec"))
)

above_average = (
    exploded.alias("s")
    .join(mood_stats.alias("g"), F.col("g.mood") == F.col("s.mood"), "inner")
    .filter(F.col("s.bpm_minus_duration_sec") >= F.col("g.median_bpm_minus_duration_sec"))
    .select(
        "s.*",
        "g.median_bpm_minus_duration_sec",
        F.round(F.col("s.bpm_minus_duration_sec") - F.col("g.median_bpm_minus_duration_sec"), 2).alias("bpm_minus_duration_sec_minus_median"),
    )
)

joined = (
    above_average.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.bpm_minus_duration_sec",
        "s.title_ref_clean",
        "s.mood_pos",
        "s.median_bpm_minus_duration_sec",
        "s.bpm_minus_duration_sec_minus_median",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_parent = (
    joined.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", "a.artist_name")
)

result = (
    with_parent
    .filter(F.upper(F.trim(F.col("artist_name"))).like("N%"))
    .select(
        "id",
        "title",
        "duration_sec",
        "bpm_minus_duration_sec",
        "mood_pos",
        "median_bpm_minus_duration_sec",
        "bpm_minus_duration_sec_minus_median",
        "artist_country",
        "artist_is_verified",
        F.initcap(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
    )
    .orderBy(
        F.col("bpm_minus_duration_sec_minus_median").desc_nulls_first(),
        F.col("bpm_minus_duration_sec").desc(),
        "id",
        "mood_pos",
    )
    .limit(10)
)
