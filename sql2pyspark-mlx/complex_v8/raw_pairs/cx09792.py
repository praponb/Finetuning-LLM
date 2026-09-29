from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_csat = Window.orderBy(F.col("ticket_csat").desc(), "id")
w_ticket_closed_at_month = Window.partitionBy(F.date_trunc("month", "ticket_closed_at"))
w_ticket_agent_id_ticket_csat = Window.partitionBy("ticket_agent_id", "ticket_csat").orderBy("avg_ticket_csat_plus_ticket_csat_percent_rank", "id", "ticket_id_2")
w_ticket_agent_id_ticket_csat_2 = Window.partitionBy("ticket_agent_id", "ticket_csat").orderBy("avg_ticket_csat_plus_ticket_csat_percent_rank")

recent = (
    df_ticket_events
    .filter(F.col("actor") != "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

joined = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id") <= 9)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor_clean",
        "s.event_type_ref",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_ticket_csat), 3).alias("ticket_csat_percent_rank"),
        F.lag("ticket_closed_at").over(w_all_by_ticket_csat).alias("prev_ticket_closed_at"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_ref")), " ", "_")).alias("event_type_ref_slug"),
        F.initcap(F.trim(F.col("event_type_ref"))).alias("event_type_ref_clean"),
        (F.col("ticket_csat") + F.col("ticket_csat_percent_rank")).alias("ticket_csat_plus_ticket_csat_percent_rank"),
    )
)

ticket_agent_id_stats = (
    annotated
    .groupBy("ticket_agent_id")
    .agg(F.avg("ticket_csat_plus_ticket_csat_percent_rank").alias("avg_ticket_csat_plus_ticket_csat_percent_rank"))
)

vs_group = (
    annotated.alias("s")
    .join(ticket_agent_id_stats.alias("g"), F.col("g.ticket_agent_id") == F.col("s.ticket_agent_id"), "inner")
    .filter(F.col("s.ticket_csat_plus_ticket_csat_percent_rank") > F.col("g.avg_ticket_csat_plus_ticket_csat_percent_rank"))
    .select(
        "s.*",
        "g.avg_ticket_csat_plus_ticket_csat_percent_rank",
        F.round(F.col("s.ticket_csat_plus_ticket_csat_percent_rank") - F.col("g.avg_ticket_csat_plus_ticket_csat_percent_rank"), 2).alias("ticket_csat_plus_ticket_csat_percent_rank_minus_"),
    )
)

with_children = (
    vs_group.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type_ref",
        "s.ticket_csat",
        "s.ticket_agent_id",
        "s.ticket_closed_at",
        "s.ticket_csat_percent_rank",
        "s.prev_ticket_closed_at",
        "s.event_type_ref_slug",
        "s.event_type_ref_clean",
        "s.avg_ticket_csat_plus_ticket_csat_percent_rank",
        "s.ticket_csat_plus_ticket_csat_percent_rank_minus_",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

with_trends = (
    with_children
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_ref",
        "ticket_csat",
        "ticket_agent_id",
        "event_type_ref_slug",
        "event_type_ref_clean",
        "avg_ticket_csat_plus_ticket_csat_percent_rank",
        "ticket_id_2",
        "ticket_closed_at_2",
        "ticket_tags",
        F.size(F.collect_set("ticket_id").over(w_ticket_closed_at_month)).alias("distinct_ticket_id_in_ticket_closed_at_month"),
        F.ntile(5).over(w_ticket_agent_id_ticket_csat).alias("avg_ticket_csat_plus_ticket_csat_percent_rank_qu"),
        F.rank().over(w_ticket_agent_id_ticket_csat).alias("avg_ticket_csat_plus_ticket_csat_percent_rank_ra"),
        F.round(F.percent_rank().over(w_ticket_agent_id_ticket_csat_2), 3).alias("avg_ticket_csat_plus_ticket_csat_percent_rank_pe"),
        F.lag("event_at", 3).over(w_ticket_agent_id_ticket_csat).alias("prev3_event_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "ticket_csat",
        "ticket_agent_id",
        "event_type_ref_clean",
        "avg_ticket_csat_plus_ticket_csat_percent_rank",
        "ticket_closed_at_2",
        "ticket_tags",
        "distinct_ticket_id_in_ticket_closed_at_month",
        "avg_ticket_csat_plus_ticket_csat_percent_rank_qu",
        "avg_ticket_csat_plus_ticket_csat_percent_rank_pe",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_ref")), " ", "_")).alias("event_type_ref_slug_2"),
        F.concat(F.col("event_type_ref_clean"), F.lit("#"), F.col("ticket_id_2").cast("string")).alias("event_type_ref_clean_ref"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
