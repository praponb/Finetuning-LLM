from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

grouped = (
    cleaned
    .groupBy(F.date_trunc("month", "release_date").alias("release_date_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.min("cnt").alias("overall_min_cnt")))
    .filter(F.col("cnt") > F.col("overall_min_cnt"))
    .drop("overall_min_cnt")
)

result = (
    screened
    .select(
        "release_date_month",
        "cnt",
        "avg_duration_sec",
        F.date_trunc("month", F.col("release_date_month")).alias("release_date_month_month"),
    )
    .orderBy("avg_duration_sec")
)
