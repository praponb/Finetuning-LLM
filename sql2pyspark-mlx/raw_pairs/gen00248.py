from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("event_time").desc(), "id")

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-05-11").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.substring("page", 1, 3).alias("page_prefix"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "session_id", "event_type", "page", "page_prefix")
)

with_dims = (
    deduped
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.session_id",
        "e.page",
        "e.page_prefix",
        F.col("p.device"),
        F.col("p.browser"),
    )
)

base = (
    with_dims
    .filter(F.col("page") == "/checkout")
    .select("*", F.lower("page").alias("page_lower"))
)

valid = (
    base
    .filter(F.col("browser").isNotNull() & (F.col("session_id") == 2))
    .select("*", F.concat("page", F.lit("/"), "browser").alias("page_browser"))
)

result = (
    valid
    .groupingSets(
        [["device", "session_id", "browser"], ["device", "session_id"], ["device"], []],
        "device",
        "session_id",
        "browser",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("page")).alias("num_pages"),
    )
)
