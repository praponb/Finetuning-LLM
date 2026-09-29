from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at_date", "id")

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2024-01-26 15:00:00"))) & F.col("browser").isin(["Edge", "Firefox"]))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
        F.to_date("started_at").alias("started_at_date"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("resolved_country"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

base_sessions = (
    not_flagged
    .filter(F.col("device").isin(["mobile", "tablet", "desktop"]))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "browser_filled",
        "started_at_date",
        "resolved_country",
        F.date_trunc("month", "started_at_date").alias("started_at_date_period"),
    )
)

prev_events = base_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

ranked = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "browser_filled",
        "started_at_date",
        "resolved_country",
        "started_at_date_period",
        "prev_started_at",
    )
    .orderBy(F.col("country").asc_nulls_last())
)
