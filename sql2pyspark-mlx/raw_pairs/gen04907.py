from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w2 = Window.partitionBy("session_id", "device")
w3 = Window.partitionBy("session_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("session_id")
w5 = Window.partitionBy("session_id").orderBy("session_id_stddev_samp_duration_sec", "id")
w6 = Window.partitionBy("session_country", "device").orderBy(F.col("session_id_stddev_samp_duration_sec").desc(), "id")

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-05-05 18:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.page",
        "e.event_time",
        "e.duration_sec",
        F.col("s.country"),
        F.col("s.device"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .filter(F.to_date(F.col("d.started_at")) >= "2025-08-01")
    .select("s.*", F.col("d.country").alias("session_country"))
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "session_id",
        "event_time",
        "country",
        "rank_in_group",
        F.stddev_samp("duration_sec").over(w2).alias("session_id_stddev_samp_duration_sec"),
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec"),
        F.stddev_samp("duration_sec").over(w_ordered).alias("session_id_stddev_samp_duration_sec_2"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.country",
        "e.session_id_stddev_samp_duration_sec",
        "e.cumulative_duration_sec",
        F.col("d.country").alias("session_country"),
        F.col("d.device"),
    )
)

result = (
    with_dims
    .withColumn("session_id_stddev_samp_duration_sec_bucket", F.ntile(3).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)
