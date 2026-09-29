from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_least_bpm_durat = Window.partitionBy("artist_id").orderBy("least_bpm_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_least_bpm_durat_2 = Window.partitionBy("artist_id").orderBy("least_bpm_duration_sec", "id")
w_is_explicit_least_bpm_durati = Window.partitionBy("is_explicit", "least_bpm_duration_sec_seq").orderBy(F.col("duration_sec").desc(), "id")
w_least_bpm_duration_sec_seq_i = Window.partitionBy("least_bpm_duration_sec_seq", "is_explicit")
w_is_explicit_least_bpm_durati_2 = Window.partitionBy("is_explicit", "least_bpm_duration_sec_seq").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_least_bpm_durati_3 = Window.partitionBy("is_explicit", "least_bpm_duration_sec_seq").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter(F.col("bpm").between(89, 180))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        "least_bpm_duration_sec",
        "duration_sec_minus_bpm",
        F.nth_value("bpm_per_duration_sec", 2).over(w_artist_id_by_least_bpm_durat).alias("second_bpm_per_duration_sec"),
        F.row_number().over(w_artist_id_by_least_bpm_durat_2).alias("least_bpm_duration_sec_seq"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.ntile(10).over(w_is_explicit_least_bpm_durati).alias("duration_sec_decile"),
        F.nth_value("duration_sec_minus_bpm", 2).over(w_is_explicit_least_bpm_durati_2).alias("second_duration_sec_minus_bpm"),
        F.first("least_bpm_duration_sec", ignorenulls=True).over(w_is_explicit_least_bpm_durati_3).alias("first_known_least_bpm_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_least_bpm_duration_sec_seq_i), F.lit(0)), 4).alias("duration_sec_share_of_least_bpm_duration_sec_seq"),
    )
)

scored = (
    with_trends
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "moods",
        "least_bpm_duration_sec",
        "duration_sec_minus_bpm",
        "second_bpm_per_duration_sec",
        "least_bpm_duration_sec_seq",
        "duration_sec_decile",
        "duration_sec_share_of_least_bpm_duration_sec_seq",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

result = (
    scored
    .select(
        "id",
        F.col("least_bpm_duration_sec_seq").alias("least_bpm_duration_sec_seq_v"),
        F.col("least_bpm_duration_sec").alias("m_least_bpm_duration_sec"),
    )
    .unpivot(
        ids=["id"],
        values=["least_bpm_duration_sec_seq_v", "m_least_bpm_duration_sec"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric_name")
)
