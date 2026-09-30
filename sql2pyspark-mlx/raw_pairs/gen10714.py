from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("web_event_page").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.hour("started_at").alias("started_at_hour"),
        F.trim("country").alias("clean_country"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.page").alias("web_event_page"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_semi")
)

scored = (
    kept
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "started_at_hour",
        "web_event_duration_sec",
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.sum("started_at_hour").over(w_part).alias("cumulative_started_at_hour"),
    )
)

kept_sessions = (
    scored
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    kept_sessions
    .groupBy("device")
    .pivot("country", ["JP", "TH", "DE"])
    .agg(F.sum("started_at_hour"))
)
