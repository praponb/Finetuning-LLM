from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser")
w4 = Window.orderBy("island_id", "id")
w5 = Window.partitionBy("session_id").orderBy("island_id", "id")

with_dims = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.device"), F.col("p.country"), F.col("p.browser"))
)

with_gaps = (
    with_dims
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

joined = (
    sessions_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.browser") == "Safari")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.page",
        "src.island_id",
        F.col("b.browser").alias("session_browser"),
    )
)

enriched = (
    joined
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.session_id",
        "e.event_type",
        "e.island_id",
        F.col("d.country"),
        F.col("d.browser"),
        F.col("d.device"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "event_type",
        "island_id",
        "country",
        "device",
        F.avg("island_id").over(w3).alias("browser_avg_island_id"),
        F.ntile(3).over(w4).alias("island_id_bucket"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

cleaned = (
    metrics
    .filter(~F.col("device").eqNullSafe(F.lit("desktop")))
    .select(
        "id",
        "island_id",
        "country",
        "device",
        "browser_avg_island_id",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "island_id",
        "country",
        "device",
        "browser_avg_island_id",
        "event_type_title",
    )
    .orderBy(F.col("island_id").desc())
)
