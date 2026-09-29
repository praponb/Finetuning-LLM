from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "device").orderBy("session_no", "id")

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

deduped = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "device", "browser", "started_at", "session_no")
)

kept = (
    deduped
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

cleaned = (
    not_flagged
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-08-30 12:00:00"))) & F.col("browser").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.initcap(F.trim("device")).alias("device_title"),
    )
)

filtered = (
    cleaned
    .filter(F.col("browser").isNotNull() & F.col("days_since_started_at").between(21, 35))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "session_no",
        "device_title",
        (F.col("days_since_started_at") - F.col("session_no")).alias("days_since_started_at_minus_session_no"),
    )
)

result = (
    filtered
    .select("id", "device", "browser", "started_at", "session_no", "device_title")
)
