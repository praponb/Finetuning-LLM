from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("event_type", "session_id").orderBy("duration_sec", "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-04").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.when(F.col("duration_sec") >= 402, "High").when(F.col("duration_sec") >= 174, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

enriched = (
    filtered
    .filter(F.col("session_id") == 7)
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.lead("duration_sec").over(w_recent).alias("next_duration_sec"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.session_id",
        "e.next_duration_sec",
        "e.seq_no",
        F.col("p.device"),
    )
)

enriched_web_events = (
    joined
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.country"), F.col("p.browser"))
)

result = (
    enriched_web_events
    .alias("a")
    .join(df_sessions.alias("dim"), F.col("a.session_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["session_browser", "session_country"], ["session_browser"]],
        F.col("dim.browser").alias("session_browser"),
        F.col("dim.country").alias("session_country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.seq_no")).alias("avg_seq_no"),
        F.round(F.sum(F.col("a.next_duration_sec")), 2).alias("total_next_duration_sec"),
    )
    .orderBy("rows_total")
)
