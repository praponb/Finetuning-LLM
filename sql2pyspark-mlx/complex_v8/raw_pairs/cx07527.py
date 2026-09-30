from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_listener_country = Window.partitionBy("listener_country")
w_source_listener_id_by_played = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").desc())
w_all_by_played_at_prior_3 = Window.orderBy("played_at", "id").rowsBetween(-3, -1)
w_listener_country_by_listener = Window.partitionBy("listener_country").orderBy("listener_birth_year")
w_all_by_listener_birth_year_t = Window.orderBy(F.col("listener_birth_year").desc())
w_source_listener_id_by_played_2 = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, -1)
w_listener_country_by_listener_2 = Window.partitionBy("listener_country").orderBy("listener_birth_year", "id")
w_listener_country_by_listener_3 = Window.partitionBy("listener_country").orderBy("listener_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_plays
    .filter((F.col("ms_played") <= 257789) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
    )
)

enriched_listener = (
    base.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.device_prefix",
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_trends = (
    enriched_listener
    .select(
        "id",
        "track_id",
        "device",
        "device_prefix",
        "listener_birth_year",
        "listener_plan",
        F.sum("listener_birth_year").over(w_track_id).alias("track_id_sum_listener_birth_year"),
        F.sum("listener_birth_year").over(w_listener_country).alias("listener_country_sum_listener_birth_year"),
        F.dense_rank().over(w_source_listener_id_by_played).alias("played_at_dense_rank"),
        F.max("listener_birth_year").over(w_track_id).alias("track_id_max_listener_birth_year"),
        F.max("listener_birth_year").over(w_all_by_played_at_prior_3).alias("listener_birth_year_max_3_prior"),
        F.round(F.cume_dist().over(w_listener_country_by_listener), 3).alias("listener_birth_year_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_listener_birth_year_t), 3).alias("listener_birth_year_cume_dist_2"),
        F.round(F.percent_rank().over(w_all_by_listener_birth_year_t), 3).alias("listener_birth_year_percent_rank"),
        F.sum("listener_birth_year").over(w_source_listener_id_by_played_2).alias("listener_birth_year_sum_2_prior"),
        (F.col("listener_birth_year") - F.lag("listener_birth_year").over(w_listener_country_by_listener_2)).alias("listener_birth_year_change"),
        F.nth_value("listener_birth_year", 2).over(w_listener_country_by_listener_3).alias("second_listener_birth_year"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("played_at_dense_rank") <= 3)
    .drop("played_at_dense_rank")
)

result = (
    top_rows
    .filter(F.col("listener_birth_year_percent_rank") >= 0.1)
    .select(
        "id",
        "track_id",
        "device",
        "device_prefix",
        "listener_birth_year",
        "listener_plan",
        "track_id_sum_listener_birth_year",
        "track_id_max_listener_birth_year",
        "listener_birth_year_max_3_prior",
        "listener_birth_year_cume_dist",
        "listener_birth_year_cume_dist_2",
        "listener_birth_year_percent_rank",
        "listener_birth_year_sum_2_prior",
        "listener_birth_year_change",
        "second_listener_birth_year",
        F.concat_ws("|", F.col("device_prefix"), F.col("device")).alias("device_prefix_device_label"),
    )
    .orderBy("track_id_max_listener_birth_year")
)
