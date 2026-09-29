from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())
w_release_date_week_day_month = Window.partitionBy(F.month("release_date_week_day"))
w_all_by_total_duration_sec = Window.orderBy(F.col("total_duration_sec").desc(), "release_date_week_day", "release_date_ym")
w_all_by_total_duration_sec_pr = Window.orderBy(F.col("total_duration_sec").desc(), "release_date_week_day", "release_date_ym").rowsBetween(-3, -1)
w_all_by_total_duration_sec_ti = Window.orderBy(F.col("total_duration_sec").desc())

valid = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

unique_rows = (
    valid
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

summarized_rows = (
    unique_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at")
)

per_group = (
    summarized_rows
    .groupBy(
        F.to_date("release_date_week").alias("release_date_week_day"),
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

screened = (
    per_group
    .crossJoin(per_group.agg(F.max("avg_duration_sec").alias("overall_max_avg_duration_sec")))
    .filter(F.col("avg_duration_sec") < F.col("overall_max_avg_duration_sec"))
    .drop("overall_max_avg_duration_sec")
)

sequenced = (
    screened
    .select(
        "*",
        F.count(F.lit(1)).over(w_release_date_week_day_month).alias("rows_in_release_date_week_day_month"),
        F.lead("avg_duration_sec", 2).over(w_all_by_total_duration_sec).alias("next2_avg_duration_sec"),
        F.max("total_duration_sec").over(w_release_date_week_day_month).alias("release_date_week_day_month_max_total_duration_s"),
        F.row_number().over(w_all_by_total_duration_sec).alias("total_duration_sec_seq"),
        F.max("total_duration_sec").over(w_all_by_total_duration_sec_pr).alias("total_duration_sec_max_3_prior"),
        F.dense_rank().over(w_all_by_total_duration_sec_ti).alias("total_duration_sec_dense_rank"),
    )
)

derived = (
    sequenced
    .select(
        "*",
        F.dayofweek(F.col("release_date_week_day")).alias("release_date_week_day_dayofweek"),
        F.round(F.col("total_duration_sec") / F.nullif(F.col("total_duration_sec_seq"), F.lit(0)), 3).alias("total_duration_sec_per_total_duration_sec_seq"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date_week_day")).alias("days_since_release_date_week_day"),
    )
)

result = (
    derived
    .select(
        "release_date_week_day",
        "release_date_ym",
        "avg_duration_sec",
        "total_duration_sec",
        "next2_avg_duration_sec",
        "total_duration_sec_per_total_duration_sec_seq",
        F.length(F.trim(F.col("release_date_ym"))).alias("release_date_ym_len"),
    )
    .orderBy(F.col("release_date_week_day").desc(), "release_date_ym")
    .limit(10)
)
