from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("web_event_event_time", "id")
w6 = Window.partitionBy("customer_id").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page").isNotNull()), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

enriched = (
    with_latest
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w_running).alias("first_known_country"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser"),
        F.last("browser", ignorenulls=True).over(w4).alias("last_known_browser_2"),
    )
)

gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.col("web_event_event_time") - F.lag("web_event_event_time").over(w5) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("session_start_flag").over(w6))

not_flagged_sessions = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.web_event_event_time")), "left_anti")
)

result = (
    not_flagged_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "first_known_country",
        "session_start_flag",
        "session_id",
        F.when(F.col("browser").rlike("^(Safari|Firefox)$"), "EXTENDED").otherwise("PRIORITY").alias("browser_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("min_subscription_monthly_fee").desc())
)
