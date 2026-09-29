from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id")
w3 = Window.partitionBy("web_event_event_type").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("web_event_event_type").orderBy("burst_id", "id")
w5 = Window.partitionBy("web_event_event_type").orderBy("started_at_day", "id")

valid = (
    df_sessions
    .filter(F.col("country") != "SG")
    .select("*", F.initcap(F.trim("browser")).alias("browser_title"))
)

with_last = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.started_at",
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

gaps = (
    with_last
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device",
        "started_at",
        "web_event_event_type",
        F.sum("is_new_session").over(w3).alias("burst_id"),
    )
)

valid_sessions = (
    islands
    .filter(F.col("web_event_event_type").isNotNull() & (F.to_date("started_at") >= "2024-06-10"))
    .select(
        "id",
        "device",
        "started_at",
        "web_event_event_type",
        "burst_id",
        F.dayofmonth("started_at").alias("started_at_day"),
        F.coalesce("web_event_event_type", F.lit("N/A")).alias("web_event_event_type_clean"),
    )
)

metrics = (
    valid_sessions
    .select(
        "id",
        "device",
        "web_event_event_type",
        "burst_id",
        "started_at_day",
        F.lag("burst_id", 1, 0).over(w_ordered).alias("prev_burst_id"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "web_event_event_type",
        "burst_id",
        "started_at_day",
        "prev_burst_id",
        "seq_no",
    )
)
