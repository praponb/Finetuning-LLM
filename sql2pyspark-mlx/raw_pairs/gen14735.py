from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "started_at"), "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-09-24").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country_2"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < "2025-03-01"), "left_anti")
)

enriched = (
    kept
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.device",
        "e.started_at",
        "e.country",
        "e.started_at_label",
        "e.resolved_browser",
        "e.first_known_country_2",
        F.col("d.country").alias("customer_country"),
    )
)

with_window = (
    enriched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

not_flagged = (
    with_window
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (~F.col("ch.event_type").isin(["search", "click"])), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "started_at",
        "started_at_label",
        "first_known_country_2",
        "subscription_id",
        "plan",
        "monthly_fee",
    )
)
