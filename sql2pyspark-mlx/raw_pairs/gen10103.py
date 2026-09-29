from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id", "subscription_id")
w2 = Window.partitionBy("device").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((~F.col("browser").eqNullSafe(F.lit("Edge"))) & (F.to_date("started_at") >= "2025-10-01"))
    .withColumn("country_upper", F.upper("country"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("UNKNOWN")))
)

kept = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page").isin(["/product", "/cart"])), "left_anti")
)

with_window = (
    not_flagged
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_gaps = (
    with_window
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country_upper",
        "subscription_id",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

eligible = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

by_type = (
    eligible
    .groupBy("browser")
    .pivot("device", ["desktop", "mobile"])
    .agg(F.sum("island_id").alias("total"))
)

result = (
    by_type
    .select(
        "browser",
        "desktop",
        F.when(F.col("browser").rlike("^(Edge|Safari)$"), "GROUP_A").when(F.col("browser").rlike("^(Chrome|Firefox)$"), "CORE").otherwise("OTHER_ZONE").alias("browser_zone"),
    )
    .orderBy(F.col("desktop").asc_nulls_first())
)
