from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_last_event_at_ties = Window.orderBy(F.col("last_event_at").desc())
w_actor_by_event_at_running = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_event_at_full = Window.orderBy(F.col("last_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_last_event_at = Window.partitionBy("ticket_id").orderBy(F.col("last_event_at").desc(), "id")

scoped_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(10, 14)) & (F.col("event_type") != "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_counts = (
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.last_event_at")
)

ranked = (
    with_counts
    .select(
        "id",
        "ticket_id",
        "actor",
        F.round(F.percent_rank().over(w_all_by_last_event_at_ties), 3).alias("last_event_at_percent_rank"),
        F.last("last_event_at", ignorenulls=True).over(w_actor_by_event_at_running).alias("last_known_last_event_at"),
        F.first("event_at").over(w_all_by_last_event_at_full).alias("first_event_at_2"),
        F.round(F.cume_dist().over(w_actor_by_event_at), 3).alias("event_at_cume_dist"),
        F.row_number().over(w_ticket_id_by_last_event_at).alias("last_event_at_seq"),
        F.first("first_event_at").over(w_all_by_last_event_at_full).alias("first_first_event_at"),
    )
)

enriched = (
    ranked
    .filter(F.hour(F.col("last_known_last_event_at")).between(7, 9))
    .select(
        "*",
        F.least(F.col("event_at_cume_dist"), F.col("last_event_at_seq")).alias("least_event_at_cume_dist_last_event_at_seq"),
        F.round(F.col("event_at_cume_dist") * 1.1, 2).alias("event_at_cume_dist_adj"),
    )
)

actor_stats = (
    enriched
    .groupBy("actor")
    .agg(F.percentile_approx("event_at_cume_dist", 0.5).alias("median_event_at_cume_dist"))
)

compared = (
    enriched.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_at_cume_dist") >= F.col("g.median_event_at_cume_dist"))
    .select(
        "s.*",
        "g.median_event_at_cume_dist",
        F.round(F.col("s.event_at_cume_dist") - F.col("g.median_event_at_cume_dist"), 2).alias("event_at_cume_dist_minus_median"),
    )
)

line_level_events = (
    compared.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.last_event_at_percent_rank",
        "s.last_known_last_event_at",
        "s.first_event_at_2",
        "s.event_at_cume_dist",
        "s.last_event_at_seq",
        "s.least_event_at_cume_dist_last_event_at_seq",
        "s.median_event_at_cume_dist",
        "s.event_at_cume_dist_minus_median",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

with_activity = (
    line_level_events
    .crossJoin(line_level_events.agg(F.min("last_event_at_percent_rank").alias("overall_min_last_event_at_percent_rank")))
    .filter(F.col("last_event_at_percent_rank") > F.col("overall_min_last_event_at_percent_rank"))
    .drop("overall_min_last_event_at_percent_rank")
)

result = (
    with_activity
    .select(
        "id",
        "ticket_id",
        "actor",
        "last_event_at_percent_rank",
        "last_known_last_event_at",
        "last_event_at_seq",
        "event_at_cume_dist_minus_median",
        "event_id",
        "event_type",
        "event_at",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.round(F.col("event_at_cume_dist") * 1.5, 2).alias("event_at_cume_dist_adj"),
    )
    .orderBy(F.col("event_at").desc_nulls_first())
)
