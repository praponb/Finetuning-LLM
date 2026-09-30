from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_plus_6m_year = Window.partitionBy(F.year("release_date_plus_6m"))
w_release_date_plus_6m_year_by = Window.partitionBy(F.year("release_date_plus_6m")).orderBy("release_date_plus_6m")
w_release_date_plus_6m_year_by_2 = Window.partitionBy(F.year("release_date_plus_6m")).orderBy("release_date_plus_6m", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%ci%")) & (F.col("bpm") >= 75))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.add_months(F.col("release_date"), 6).alias("release_date_plus_6m"),
    )
)

sequenced = (
    base
    .select(
        "id",
        "title",
        "bpm",
        "moods",
        F.size(F.collect_set("artist_id").over(w_release_date_plus_6m_year)).alias("distinct_artist_id_in_release_date_plus_6m_year"),
        F.dense_rank().over(w_release_date_plus_6m_year_by).alias("release_date_plus_6m_dense_rank"),
        F.sum("bpm").over(w_release_date_plus_6m_year).alias("release_date_plus_6m_year_sum_bpm"),
        F.max("duration_sec").over(w_release_date_plus_6m_year).alias("release_date_plus_6m_year_max_duration_sec"),
        F.max("duration_sec").over(w_release_date_plus_6m_year_by_2).alias("duration_sec_max_7"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

derived = (
    qualified
    .select(
        "*",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.size(F.col("moods")).alias("moods_count"),
        F.greatest(F.col("duration_sec_max_7"), F.col("bpm")).alias("greatest_duration_sec_max_7_bpm"),
    )
)

per_label = (
    derived
    .select(
        "id",
        "title",
        "bpm",
        "distinct_artist_id_in_release_date_plus_6m_year",
        "release_date_plus_6m_dense_rank",
        "release_date_plus_6m_year_sum_bpm",
        "release_date_plus_6m_year_max_duration_sec",
        "duration_sec_max_7",
        "moods_text",
        "moods_count",
        "greatest_duration_sec_max_7_bpm",
        F.explode_outer("moods").alias("mood"),
    )
)

grouped = (
    per_label
    .groupBy("moods_count", "mood")
    .agg(
        F.round(F.stddev("moods_count"), 2).alias("stddev_moods_count"),
        F.sum("greatest_duration_sec_max_7_bpm").alias("total_greatest_duration_sec_max_7_bpm"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.sum("release_date_plus_6m_year_max_duration_sec").alias("total_release_date_plus_6m_year_max_duration_sec"),
        F.min("distinct_artist_id_in_release_date_plus_6m_year").alias("min_distinct_artist_id_in_release_date_plus_6m_y"),
        F.sum("release_date_plus_6m_dense_rank").alias("sum_release_date_plus_6m_dense_rank_check"),
    )
    .filter(F.col("sum_release_date_plus_6m_dense_rank_check") > 4)
    .drop("sum_release_date_plus_6m_dense_rank_check")
)

tagged = (
    grouped
    .select(
        "moods_count",
        "mood",
        "stddev_moods_count",
        "total_greatest_duration_sec_max_7_bpm",
        "title_list",
        "total_release_date_plus_6m_year_max_duration_sec",
        F.least(F.col("moods_count"), F.col("stddev_moods_count")).alias("least_moods_count_stddev_moods_count"),
        F.round(F.col("stddev_moods_count") * 0.9, 2).alias("stddev_moods_count_adj"),
    )
)

result = (
    tagged
    .select(
        "moods_count",
        "mood",
        "stddev_moods_count",
        "total_greatest_duration_sec_max_7_bpm",
        "title_list",
        "least_moods_count_stddev_moods_count",
        "stddev_moods_count_adj",
    )
    .orderBy(F.col("stddev_moods_count").desc(), F.col("mood").desc())
)
