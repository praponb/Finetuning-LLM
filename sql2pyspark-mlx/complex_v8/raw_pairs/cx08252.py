from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_release_date_month = Window.partitionBy(F.month("last_release_date"))
w_all_by_last_release_date_tie = Window.orderBy("last_release_date")
w_all_by_last_release_date_pri = Window.orderBy("last_release_date", "release_date_ym", "artist_id").rowsBetween(-1, -1)

filtered_tracks = (
    df_tracks
    .filter((F.col("duration_sec").between(165, 357)) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
    )
)

per_group = (
    filtered_tracks
    .filter(F.col("release_date") >= F.lit("2024-06-03").cast("date"))
    .groupBy(
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
        "artist_id",
    )
    .agg(
        F.max("release_date").alias("last_release_date"),
        F.count("*").alias("cnt"),
        F.round(F.avg("moods_count"), 2).alias("avg_moods_count"),
    )
)

with_windows = (
    per_group
    .select(
        "release_date_ym",
        "artist_id",
        "last_release_date",
        "cnt",
        "avg_moods_count",
        F.max("cnt").over(w_last_release_date_month).alias("last_release_date_month_max_cnt"),
        F.round(F.col("avg_moods_count") - F.avg("avg_moods_count").over(w_last_release_date_month), 2).alias("avg_moods_count_vs_last_release_date_month_avg"),
        F.rank().over(w_all_by_last_release_date_tie).alias("last_release_date_rank"),
        F.round(F.avg("cnt").over(w_all_by_last_release_date_pri), 2).alias("cnt_avg_1_prior"),
    )
)

result = (
    with_windows
    .select(
        "release_date_ym",
        "artist_id",
        F.col("avg_moods_count_vs_last_release_date_month_avg").alias("m_avg_moods_count_vs_last_release_date_month_avg"),
        F.col("cnt_avg_1_prior").alias("m_cnt_avg_1_prior"),
        F.col("avg_moods_count").alias("m_avg_moods_count"),
    )
    .unpivot(
        ids=["release_date_ym", "artist_id"],
        values=["m_avg_moods_count_vs_last_release_date_month_avg", "m_cnt_avg_1_prior", "m_avg_moods_count"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("release_date_ym", "artist_id", "metric")
)
