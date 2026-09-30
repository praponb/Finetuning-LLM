from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_listener_signup_da = Window.partitionBy("device").orderBy(F.col("listener_signup_date").desc(), "id")
w_listener_country_source_by_t = Window.partitionBy("listener_country", "source").orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_listener_country = Window.partitionBy("track_id", "listener_country")
w_listener_id_listener_country = Window.partitionBy("listener_id", "listener_country").orderBy("track_duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_listener_id_listener_country_2 = Window.partitionBy("listener_id", "listener_country").orderBy("track_duration_sec", "id")
w_listener_country_source_by_t_2 = Window.partitionBy("listener_country", "source").orderBy(F.col("track_duration_sec").desc(), "id")
w_source = Window.partitionBy("source")
w_device_by_listener_signup_da_2 = Window.partitionBy("device").orderBy(F.col("listener_signup_date").desc())
w_track_id_source = Window.partitionBy("track_id", "source")
w_listener_signup_date_month_b = Window.partitionBy(F.date_trunc("month", "listener_signup_date")).orderBy("listener_signup_date", "id").rowsBetween(-2, -1)

valid = (
    df_plays
    .filter((F.col("listener_id") <= 5) & (F.col("device").isin("car", "tv")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
        F.abs(F.col("ms_played") - 166517).alias("ms_played_distance"),
    )
)

joined_listener = (
    valid.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") <= 1980), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

with_trends = (
    joined_listener
    .select(
        "*",
        F.lag("track_duration_sec").over(w_device_by_listener_signup_da).alias("prev_track_duration_sec"),
        F.last("track_duration_sec").over(w_listener_country_source_by_t).alias("last_track_duration_sec"),
        F.count(F.lit(1)).over(w_track_id_listener_country).alias("rows_in_track_id_listener_country"),
        F.max("track_duration_sec").over(w_listener_id_listener_country).alias("track_duration_sec_max_3"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_track_id_listener_country), F.lit(0)), 4).alias("track_duration_sec_share_of_track_id_listener_co"),
        F.round(F.percent_rank().over(w_listener_id_listener_country_2), 3).alias("track_duration_sec_percent_rank"),
        F.dense_rank().over(w_listener_country_source_by_t_2).alias("track_duration_sec_dense_rank"),
        F.max("listener_birth_year").over(w_source).alias("source_max_listener_birth_year"),
        F.lead("listener_birth_year").over(w_device_by_listener_signup_da).alias("next_listener_birth_year"),
        F.rank().over(w_device_by_listener_signup_da_2).alias("listener_signup_date_rank"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_source), 2).alias("listener_birth_year_vs_source_avg"),
        F.count(F.lit(1)).over(w_track_id_source).alias("rows_in_track_id_source"),
        F.sum("listener_birth_year").over(w_listener_signup_date_month_b).alias("listener_birth_year_sum_2_prior"),
    )
)

result = (
    with_trends
    .filter(F.col("listener_country") == "UK")
    .select(
        "id",
        "last_track_duration_sec",
        "rows_in_track_id_listener_country",
        "track_duration_sec_max_3",
        "track_duration_sec_share_of_track_id_listener_co",
        "track_duration_sec_percent_rank",
        "next_listener_birth_year",
        "listener_birth_year_vs_source_avg",
        "rows_in_track_id_source",
        "listener_birth_year_sum_2_prior",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.when(F.col("prev_track_duration_sec") >= 352, "A").when(F.col("prev_track_duration_sec") >= 183, "B").otherwise("C").alias("prev_track_duration_sec_tier"),
    )
    .orderBy(
        F.col("track_duration_sec_share_of_track_id_listener_co").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
