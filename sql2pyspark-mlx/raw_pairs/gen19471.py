from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("started_at") >= F.to_date(F.lit("2023-06-11")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.when(F.col("device").isin(["mobile", "desktop"]), "Core").otherwise("Other").alias("device_group"),
        F.when(F.col("browser").isin(["Chrome", "Edge"]), "Tier 1").otherwise("Other").alias("browser_group"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

metrics = (
    with_last
    .filter(F.col("browser").isin(["Chrome", "Firefox", "Edge"]))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "browser_group",
        "resolved_browser",
        "web_event_event_type",
        F.first("browser", ignorenulls=True).over(w_running).alias("first_known_browser"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.segment"), F.col("p.email"), F.col("p.name"))
)

gaps = (
    joined
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w4))

with_latest = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec").alias("web_event_duration_sec"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "browser_group",
        "web_event_event_type",
        "first_known_browser",
        "name",
        "session_start_flag",
        F.when(F.col("name").rlike("^(Noah Park|Mia Rossi)$"), "GROUP_B").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "GROUP_A").otherwise("PRIORITY").alias("name_zone"),
    )
    .orderBy(F.col("name_zone").desc_nulls_first())
)
