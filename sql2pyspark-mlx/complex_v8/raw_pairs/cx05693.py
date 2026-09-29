from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_source_by_track_dur = Window.partitionBy("track_id", "source").orderBy(F.col("track_duration_sec").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_listener_country = Window.partitionBy("listener_country")
w_track_id_source_by_track_dur_2 = Window.partitionBy("track_id", "source").orderBy(F.col("track_duration_sec").asc_nulls_last(), "id")
w_listener_country_by_track_du = Window.partitionBy("listener_country").orderBy(F.col("track_duration_sec").desc(), "id")
w_track_id_source_by_track_dur_3 = Window.partitionBy("track_id", "source").orderBy(F.col("track_duration_sec").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_track_id_by_track_duration_s = Window.partitionBy("track_id").orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_listener_signup_date = Window.orderBy("listener_signup_date", "id").rowsBetween(-1, -1)

valid = (
    df_plays
    .filter(~F.col("device").isin("car", "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
    )
)

with_details_listener = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        F.col("l.country").alias("listener_country"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

ranked = (
    with_details_listener
    .select(
        "*",
        F.max("track_duration_sec").over(w_track_id_source_by_track_dur).alias("track_duration_sec_max_2"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_listener_country), F.lit(0)), 4).alias("track_duration_sec_share_of_listener_country"),
        F.count(F.lit(1)).over(w_listener_country).alias("rows_in_listener_country"),
        F.round(F.percent_rank().over(w_track_id_source_by_track_dur_2), 3).alias("track_duration_sec_percent_rank"),
        F.row_number().over(w_track_id_source_by_track_dur_2).alias("track_duration_sec_seq"),
        F.ntile(3).over(w_listener_country_by_track_du).alias("track_duration_sec_tercile"),
        F.sum("track_duration_sec").over(w_track_id_source_by_track_dur_3).alias("track_duration_sec_sum_3_prior"),
        F.nth_value("track_duration_sec", 2).over(w_track_id_by_track_duration_s).alias("second_track_duration_sec"),
        F.round(F.cume_dist().over(w_listener_country_by_track_du), 3).alias("track_duration_sec_cume_dist"),
        F.round(F.avg("track_duration_sec").over(w_all_by_listener_signup_date), 2).alias("track_duration_sec_avg_1_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "listener_country",
        "listener_signup_date",
        "track_title",
        "track_duration_sec",
        "track_duration_sec_share_of_listener_country",
        "rows_in_listener_country",
        "track_duration_sec_seq",
        "track_duration_sec_tercile",
        "track_duration_sec_sum_3_prior",
        "second_track_duration_sec",
        "track_duration_sec_cume_dist",
        "track_duration_sec_avg_1_prior",
        F.when(F.col("track_duration_sec_max_2") >= 1026, "large").when(F.col("track_duration_sec_max_2") >= 441, "medium").otherwise("small").alias("track_duration_sec_max_2_tier"),
    )
    .orderBy(F.col("id").desc())
)
