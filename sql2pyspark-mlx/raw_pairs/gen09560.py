from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("started_at"), "device").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("started_at", "id", "subscription_id")
w5 = Window.partitionBy("browser").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("event_seq_2").desc(), "id", "subscription_id")
w7 = Window.partitionBy("browser").orderBy(F.col("subscription_id").desc())

enriched = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser"),
    )
)

base = (
    enriched
    .filter(F.col("event_seq") > 3)
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "event_seq",
        "event_seq_2",
        F.round((F.col("event_seq_2") / F.col("event_seq")), 2).alias("event_seq_2_per_event_seq"),
        F.to_date("started_at").alias("started_at_date"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_clean"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

matched = (
    not_flagged
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at_date") >= F.col("sub.start_date")) & (F.col("a.started_at_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

flagged = (
    matched
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w5))

metrics = (
    islands
    .filter(F.col("plan").isin(["standard", "premium", "basic"]))
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq_3"),
        F.cume_dist().over(w7).alias("subscription_id_cume_dist"),
    )
)

result = (
    metrics
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["city", "browser"], ["city"]],
        F.col("c.city").alias("city"),
        F.col("a.browser").alias("browser"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.event_seq_2_per_event_seq")).alias("min_event_seq_2_per_event_seq"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("num_rows").desc())
)
