from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").desc_nulls_first())
w3 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("session_id")
w_hist = Window.orderBy(F.col("duration_sec_count_7d_plus_duration_sec_dense_rank").desc_nulls_last())
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

kept_keys = (
    df_web_events
    .select("session_id", "id")
    .intersect(
        df_web_events
        .filter(F.col("event_time") >= "2023-12-01")
        .select("session_id", "id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "id",
        "session_id",
        F.rank().over(w).alias("duration_sec_rank"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
        F.count("duration_sec").over(w3).alias("duration_sec_count_7d"),
    )
)

cleaned = (
    enriched
    .filter(F.col("duration_sec_dense_rank") > 7)
    .withColumn("duration_sec_count_7d_plus_duration_sec_dense_rank", (F.col("duration_sec_count_7d") + F.col("duration_sec_dense_rank")))
)

scored = (
    cleaned
    .select(
        "*",
        F.round((F.col("duration_sec_dense_rank") / F.sum("duration_sec_dense_rank").over(w4)), 4).alias("duration_sec_dense_rank_share"),
        F.percent_rank().over(w_hist).alias("duration_sec_count_7d_plus_duration_sec_dense_rank_percent_rank"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.country"), F.col("s.device"))
)

result = (
    with_dims
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "duration_sec_rank",
        "duration_sec_dense_rank",
        "duration_sec_count_7d",
        "duration_sec_dense_rank_share",
        "device",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "EXTENDED").when(F.col("device").rlike("^(mobile)$"), "PRIORITY").otherwise("CORE").alias("device_zone"),
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("duration_sec_dense_rank_share").asc_nulls_first())
)
