from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id", "subscription_id")
w2 = Window.partitionBy("country").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser", "plan").orderBy("island_id", "id", "subscription_id").rowsBetween(-2, -1)
w4 = Window.partitionBy("country").orderBy("is_new_session", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country")
w6 = Window.orderBy("subscription_id_avg_2_prior", "id", "subscription_id")

with_window = (
    df_sessions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

eligible = (
    with_window
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

flagged = (
    not_flagged
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

metrics = (
    sessions_sessions
    .select(
        "*",
        F.avg("subscription_id").over(w3).alias("subscription_id_avg_2_prior"),
        F.sum("island_id").over(w4).alias("running_island_id"),
    )
)

not_flagged_sessions = (
    metrics
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_semi")
)

scored = (
    not_flagged_sessions
    .select(
        "*",
        F.max("running_island_id").over(w5).alias("country_max_running_island_id"),
        F.ntile(3).over(w6).alias("subscription_id_avg_2_prior_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "device",
        "country",
        "subscription_id",
        "plan",
        "monthly_fee",
        "is_new_session",
        "running_island_id",
        "subscription_id_avg_2_prior_bucket",
        F.when(F.col("plan").rlike("^(standard|basic)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("plan_zone"),
    )
)
