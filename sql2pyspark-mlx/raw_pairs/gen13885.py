from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("event_time").desc_nulls_first(), "id")

cleaned = (
    df_web_events
    .filter((F.col("event_type") != "add_to_cart") & (F.col("event_time") >= F.to_timestamp(F.lit("2023-08-10 08:00:00"))))
    .select("*", F.coalesce("page", F.lit("unassigned")).alias("page_filled"))
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.page",
        "src.event_time",
        "src.duration_sec",
        "src.page_filled",
        "src.dedup_rank",
        F.col("p.country"),
        F.col("p.device"),
        F.col("p.browser"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.country") == "JP")
    .select(
        "src.*",
        F.col("d.country").alias("session_country"),
        F.col("d.device").alias("session_device"),
    )
)

filtered = (
    with_dims
    .filter(F.col("browser").isNotNull())
    .withColumn("page_title", F.initcap(F.trim("page")))
)

with_dims_web_events = (
    filtered
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.page",
        "s.event_time",
        "s.duration_sec",
        "s.dedup_rank",
        "s.country",
        "s.session_device",
        "s.page_title",
        F.col("b.browser").alias("session_browser"),
        F.col("b.country").alias("session_country_2"),
        F.col("b.device").alias("session_device_2"),
    )
)

stats = (
    with_dims_web_events
    .groupBy("page")
    .agg(
        F.size(F.collect_set("session_device")).alias("num_session_devices"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("session_country_2") == "US", 1)).alias("us_rows"),
    )
)

result = (
    stats
    .select("page", "num_session_devices", "avg_duration_sec", "us_rows")
    .orderBy(F.col("page").desc())
)
