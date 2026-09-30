from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_mo = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_dedup = Window.partitionBy("artist_id").orderBy("release_date_month", "id")
w_artist_id_by_release_date_mo = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(-3, Window.currentRow)
w_all_by_duration_sec_vs_artis = Window.orderBy(F.col("duration_sec_vs_artist_id_avg").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_duration_sec_vs_artis_2 = Window.orderBy(F.col("duration_sec_vs_artist_id_avg").asc_nulls_last(), "id")
w_rn_release_date_year = Window.partitionBy("rn", F.year("release_date"))
w_all_by_duration_sec_share_of = Window.orderBy(F.col("duration_sec_share_of_artist_id").desc_nulls_first())

prepared_tracks = (
    df_tracks
    .filter(F.col("bpm").between(94, 156))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

with_windows = (
    prepared_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "moods",
        "release_date_month",
        F.round(F.avg("duration_sec").over(w_artist_id_by_duration_sec_mo), 2).alias("duration_sec_avg_4"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.round((F.unix_timestamp("release_date_month") - F.unix_timestamp(F.lag("release_date_month").over(w_all_by_duration_sec))) / 60, 1).alias("minutes_since_prev_release_date_month"),
    )
)

screened = (
    with_windows.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

latest = (
    screened
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    latest
    .select(
        "*",
        F.sum("duration_sec_avg_4").over(w_artist_id).alias("artist_id_sum_duration_sec_avg_4"),
        F.round(F.avg("duration_sec_avg_4").over(w_artist_id_by_release_date_mo), 2).alias("duration_sec_avg_4_avg_4"),
        F.round(F.avg("duration_sec_avg_4").over(w_all_by_duration_sec_vs_artis), 2).alias("duration_sec_avg_4_avg_7"),
        F.row_number().over(w_all_by_duration_sec_vs_artis_2).alias("rn"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "artist_id",
        "moods",
        "release_date_month",
        "duration_sec_avg_4",
        "artist_id_sum_duration_sec_avg_4",
        "duration_sec_avg_4_avg_4",
        F.size(F.collect_set("artist_id").over(w_rn_release_date_year)).alias("distinct_artist_id_in_rn_release_date_year"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_share_of), 3).alias("duration_sec_share_of_artist_id_cume_dist"),
    )
)

joined = (
    ranked_2.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") < 2020)
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec_avg_4",
        "s.duration_sec_avg_4_avg_4",
        "s.distinct_artist_id_in_rn_release_date_year",
        "s.duration_sec_share_of_artist_id_cume_dist",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    joined
    .select(
        "id",
        F.col("artist_debut_year").alias("artist_debut_year_v"),
        F.col("distinct_artist_id_in_rn_release_date_year").alias("m_distinct_artist_id_in_rn_release_date_year"),
    )
    .unpivot(
        ids=["id"],
        values=["artist_debut_year_v", "m_distinct_artist_id_in_rn_release_date_year"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
