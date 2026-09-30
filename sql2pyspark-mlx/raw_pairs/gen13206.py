from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.to_date("started_at"), "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy(F.to_date("started_at")).orderBy("duration_sec", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("device").orderBy("seq_no", "id")
w8 = Window.partitionBy("device")
w9 = Window.partitionBy(F.month("started_at"), "device").orderBy(F.col("duration_sec").asc_nulls_first())
w10 = Window.partitionBy("web_event_page")

enriched = (
    df_sessions
    .select(
        "id",
        "device",
        "started_at",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.page").alias("web_event_page"))
)

result = (
    with_latest
    .select(
        "id",
        "device",
        "started_at",
        "seq_no",
        "seq_no_2",
        "event_seq",
        "duration_sec",
        F.avg("event_seq").over(w_recent).alias("event_seq_avg_3_prior"),
        F.row_number().over(w7).alias("seq_no_3"),
        F.round((F.col("seq_no_2") / F.sum("seq_no_2").over(w8)), 4).alias("seq_no_2_share"),
        F.percent_rank().over(w9).alias("duration_sec_percent_rank"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w10)), 4).alias("seq_no_share"),
    )
)
