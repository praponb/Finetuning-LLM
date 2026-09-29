from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter(F.col("started_at") > F.to_timestamp(F.lit("2023-05-30 17:00:00")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country").isin(["JP", "DE"]), "Tier 1").otherwise("Other").alias("country_group"),
    )
)

with_window = (
    valid
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
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
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_semi")
)

cleaned = (
    not_flagged
    .filter(F.col("browser").isin(["Edge", "Chrome", "Firefox"]))
    .withColumn("plan_upper", F.upper("plan"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.started_at",
        "s.country_group",
        "s.subscription_id",
        "s.monthly_fee",
        "s.plan_upper",
        F.col("c.event_type"),
    )
)

result = (
    with_last
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country_group",
        "subscription_id",
        "monthly_fee",
        "plan_upper",
        "event_type",
    )
    .orderBy(F.col("started_at").asc_nulls_first())
)
