from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country = Window.partitionBy("artist_country")
w_all_by_duration_sec_running = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy("bpm")
w_is_explicit_by_running_bpm_b = Window.partitionBy("is_explicit").orderBy("running_bpm_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")

filtered_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

with_parent = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.bpm_bucket",
        "s.title_ref",
        F.col("a.country").alias("artist_country"),
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.round(F.avg("bpm").over(w_artist_country), 2).alias("artist_country_avg_bpm"),
        F.sum("bpm_bucket").over(w_all_by_duration_sec_running).alias("running_bpm_bucket"),
        F.row_number().over(w_is_explicit_by_bpm).alias("seq_no"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_country), F.lit(0)), 4).alias("duration_sec_share_of_artist_country"),
        F.max("bpm_bucket").over(w_artist_country).alias("artist_country_max_bpm_bucket"),
        F.lead("duration_sec", 3).over(w_all_by_duration_sec).alias("next3_duration_sec"),
    )
)

scored = (
    sequenced
    .filter(F.col("duration_sec_share_of_artist_country") < 0.8)
    .select(
        "id",
        "title",
        "is_explicit",
        "bpm",
        "bpm_bucket",
        "artist_country_avg_bpm",
        "running_bpm_bucket",
        "duration_sec_share_of_artist_country",
        "artist_country_max_bpm_bucket",
        "next3_duration_sec",
        F.lower(F.regexp_replace(F.trim(F.col("title_ref")), " ", "_")).alias("title_ref_slug"),
        (F.col("next3_duration_sec") - F.col("bpm_bucket")).alias("next3_duration_sec_minus_bpm_bucket"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "title",
        "is_explicit",
        "bpm_bucket",
        "running_bpm_bucket",
        "duration_sec_share_of_artist_country",
        "next3_duration_sec",
        "next3_duration_sec_minus_bpm_bucket",
        F.rank().over(w_is_explicit_by_bpm_ties).alias("bpm_rank"),
        F.first("running_bpm_bucket", ignorenulls=True).over(w_is_explicit_by_running_bpm_b).alias("first_known_running_bpm_bucket"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
    )
)

leaders = (
    metrics
    .filter((F.col("bpm_rank") == 1) & (F.col("next3_duration_sec") >= 239))
    .drop("bpm_rank")
)

annotated = (
    leaders
    .filter(F.col("next3_duration_sec_minus_bpm_bucket") <= 546)
    .select(
        "id",
        "title",
        "running_bpm_bucket",
        "next3_duration_sec_minus_bpm_bucket",
        F.greatest(F.col("bpm_bucket"), F.col("next3_duration_sec")).alias("greatest_bpm_bucket_next3_duration_sec"),
        F.when(F.col("next3_duration_sec") >= 312, "HIGH").when(F.col("next3_duration_sec") >= 164, "MEDIUM").otherwise("LOW").alias("next3_duration_sec_tier"),
    )
)

result = (
    annotated
    .select(
        "id",
        "running_bpm_bucket",
        "next3_duration_sec_minus_bpm_bucket",
        (F.floor(F.col("running_bpm_bucket") / 250) * 250).alias("running_bpm_bucket_bucket"),
    )
    .orderBy(F.col("running_bpm_bucket_bucket").desc(), "id")
    .limit(5)
)
