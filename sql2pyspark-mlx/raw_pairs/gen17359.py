from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("event_type").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("web_event_page").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device").orderBy("started_at", "id")
w8 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device").orderBy(F.col("session_id").desc(), "id")

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.page").alias("web_event_page"))
)

metrics = (
    with_last
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "event_type",
        F.row_number().over(w2).alias("event_seq"),
        F.first("event_type", ignorenulls=True).over(w3).alias("first_known_event_type"),
        F.row_number().over(w4).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w5).alias("first_known_country"),
        F.first("browser", ignorenulls=True).over(w6).alias("first_known_browser"),
    )
)

with_gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w7).isNull() | (F.col("started_at") > F.lag("started_at").over(w7) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "first_known_country",
        F.sum("session_start_flag").over(w8).alias("session_id"),
    )
)

deduped = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select("id", "device", "country", "session_id")
    .orderBy(F.col("id").desc())
)
