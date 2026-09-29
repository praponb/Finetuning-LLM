from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_group_by_event_at = Window.partitionBy("event_type_group").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc())
w_event_at_quartile = Window.partitionBy("event_at_quartile")

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("created", "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.when(F.col("event_type").isin("escalated", "assigned"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
    )
)

qualified = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority").isin("P4", "P3", "P1")) & (F.month(F.col("t.closed_at")).isin(1, 3)), "left_semi")
)

metrics = (
    qualified
    .select(
        "*",
        F.ntile(10).over(w_event_type_group_by_event_at).alias("event_at_decile"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.ntile(4).over(w_event_type_group_by_event_at).alias("event_at_quartile"),
        F.lead("event_at", 3).over(w_all_by_event_at).alias("next3_event_at"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.round(F.percent_rank().over(w_ticket_id_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.count(F.lit(1)).over(w_event_at_quartile).alias("rows_in_event_at_quartile"),
    )
)

enriched_ticket = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.channel").isNotNull())
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    enriched_ticket
    .groupBy("ticket_agent_id")
    .pivot("event_type_group", ["group_a", "group_b", "other"])
    .agg(F.sum("rows_in_event_at_quartile"))
)
