from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

base = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select(
        "*",
        F.concat_ws(",", "event_type", "page").alias("event_type_page_key"),
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .filter(F.hour(F.col("b.started_at")).between(12, 18))
    .select("s.*", F.col("b.device"), F.col("b.country"))
)

enriched_web_events = (
    enriched
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.country").alias("session_country"),
        F.col("d.device").alias("session_device"),
    )
)

ranked = (
    enriched_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_type_page_key",
        "device",
        "country",
        "session_device",
    )
)

result = (
    ranked
    .select("id", "event_type", "event_type_page_key", "device", "session_device")
    .orderBy("session_device", F.col("id").desc())
)
