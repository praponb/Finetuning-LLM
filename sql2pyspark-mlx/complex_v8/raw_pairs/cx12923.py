from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt = Window.partitionBy("cnt")
w_all_by_cnt = Window.orderBy("cnt", "is_explicit")
w_n_title_is_not_null_cnt = Window.partitionBy("n_title_is_not_null", "cnt")
w_cnt_by_cnt_full = Window.partitionBy("cnt").orderBy(F.col("cnt").desc(), "is_explicit").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-10-23").cast("date")) | (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("duration_sec") - 239).alias("duration_sec_distance"),
        F.abs(F.col("bpm") - 142).alias("bpm_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

stats = (
    cleaned
    .filter(F.array_contains(F.col("moods"), "chill"))
    .groupBy("is_explicit")
    .agg(
        F.sum(F.when(F.col("title").isNotNull(), 1).otherwise(0)).alias("n_title_is_not_null"),
        F.count("*").alias("cnt"),
        F.avg("bpm_distance").alias("avg_bpm_distance_check"),
    )
    .filter(F.col("avg_bpm_distance_check") > 36)
    .drop("avg_bpm_distance_check")
)

sequenced = (
    stats
    .select(
        "*",
        F.max("n_title_is_not_null").over(w_cnt).alias("cnt_max_n_title_is_not_null"),
        F.ntile(4).over(w_all_by_cnt).alias("cnt_quartile"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.count(F.lit(1)).over(w_n_title_is_not_null_cnt).alias("rows_in_n_title_is_not_null_cnt"),
        F.nth_value("cnt_quartile", 2).over(w_cnt_by_cnt_full).alias("second_cnt_quartile"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.abs(F.col("n_title_is_not_null") - 6).alias("n_title_is_not_null_distance"),
    )
)

annotated = (
    enriched
    .filter(F.col("cnt_quartile").between(3, 10))
    .select(
        "*",
        F.when(F.col("cnt_quartile") >= 7, "HIGH").when(F.col("cnt_quartile") >= 5, "MEDIUM").otherwise("LOW").alias("cnt_quartile_tier"),
        F.when(F.col("is_explicit_flag") >= 1, "A").when(F.col("is_explicit_flag") >= 0, "B").otherwise("C").alias("is_explicit_flag_tier"),
    )
)

result = (
    annotated
    .select(
        "is_explicit",
        "n_title_is_not_null",
        "cnt",
        "cnt_max_n_title_is_not_null",
        "cnt_quartile",
        "rows_in_n_title_is_not_null_cnt",
        "second_cnt_quartile",
        "is_explicit_flag",
        "n_title_is_not_null_distance",
        "is_explicit_flag_tier",
    )
    .orderBy(F.col("cnt_quartile").desc(), "is_explicit")
    .limit(5)
)
