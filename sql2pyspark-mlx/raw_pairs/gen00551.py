from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time_hour").asc_nulls_first())
w2 = Window.partitionBy("session_id")
w3 = Window.partitionBy("device")
w4 = Window.orderBy(F.col("duration_sec").desc())
w5 = Window.partitionBy("device").orderBy(F.col("device_max_duration_sec").desc())
w_grp = Window.orderBy(F.col("event_time_hour").desc_nulls_first())
w_grp_2 = Window.partitionBy("device").orderBy(F.col("session_id_avg_event_time_hour").desc())
w8 = Window.partitionBy("device").orderBy(F.col("session_id_min_duration_sec").desc_nulls_last())

base = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "purchase", "click"]) & (F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.hour("event_time").alias("event_time_hour"),
    )
)

remaining_keys = (
    base
    .select("event_type", "id")
    .exceptAll(
        df_web_events
        .filter(F.col("session_id").isin([7, 13, 28]))
        .select("event_type", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.device"), F.col("d.browser"), F.col("d.country"))
)

scored = (
    with_dims
    .filter(F.col("event_type") != "search")
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        "event_time_hour",
        "device",
        "country",
        F.cume_dist().over(w).alias("event_time_hour_cume_dist"),
        F.avg("event_time_hour").over(w2).alias("session_id_avg_event_time_hour"),
        F.max("duration_sec").over(w3).alias("device_max_duration_sec"),
        F.rank().over(w4).alias("duration_sec_rank"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "duration_sec",
        "event_time_hour",
        "device",
        "session_id_avg_event_time_hour",
        "device_max_duration_sec",
        "duration_sec_rank",
        F.percent_rank().over(w5).alias("device_max_duration_sec_percent_rank"),
        F.min("duration_sec").over(w2).alias("session_id_min_duration_sec"),
    )
)

scored_web_events = (
    with_windows
    .select(
        "id",
        "event_time_hour",
        "session_id_avg_event_time_hour",
        "device_max_duration_sec",
        "duration_sec_rank",
        "session_id_min_duration_sec",
        F.percent_rank().over(w_grp).alias("event_time_hour_percent_rank"),
        F.rank().over(w_grp_2).alias("session_id_avg_event_time_hour_rank"),
        F.rank().over(w8).alias("session_id_min_duration_sec_rank"),
        F.rank().over(w_grp_2).alias("session_id_avg_event_time_hour_rank_2"),
        F.max("duration_sec").over(w3).alias("device_max_duration_sec_2"),
    )
)

result = (
    scored_web_events
    .withColumn("event_time_hour_tier", F.when(F.col("event_time_hour") >= 68, "PREMIUM").otherwise("STANDARD"))
    .withColumn("duration_sec_rank_tier", F.when(F.col("duration_sec_rank") >= 6, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["event_time_hour_tier", "duration_sec_rank_tier"], ["event_time_hour_tier"]],
        "event_time_hour_tier",
        "duration_sec_rank_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("session_id_min_duration_sec_rank"), 2).alias("avg_session_id_min_duration_sec_rank"),
        F.grouping_id().alias("gid"),
    )
)
