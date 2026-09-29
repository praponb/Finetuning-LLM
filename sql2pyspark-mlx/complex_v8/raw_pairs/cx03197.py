from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_or_unknown = Window.partitionBy("actor_or_unknown")
w_ticket_channel_actor_by_agen = Window.partitionBy("ticket_channel", "actor").orderBy(F.col("agent_hired_on").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_actor_distinct_ticket_channe = Window.partitionBy("actor", "distinct_ticket_channel_in_actor_or_unknown").orderBy(F.col("rows_in_actor_or_unknown").desc(), "id")
w_all_by_distinct_ticket_chann = Window.orderBy("distinct_ticket_channel_in_actor_or_unknown")
w_actor_distinct_ticket_channe_2 = Window.partitionBy("actor", "distinct_ticket_channel_in_actor_or_unknown").orderBy(F.col("rows_in_actor_or_unknown").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(~F.col("actor").isin("system", "customer"), 1)).alias("event_actor_count"),
    )
)

with_stats = (
    recent_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

joined = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") == "P2"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_type_clean",
        "s.actor_or_unknown",
        "s.event_actor_count",
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.size(F.collect_set("actor").over(w_actor_or_unknown)).alias("distinct_actor_in_actor_or_unknown"),
        F.max("event_actor_count").over(w_ticket_channel_actor_by_agen).alias("event_actor_count_max_3"),
        F.size(F.collect_set("ticket_channel").over(w_actor_or_unknown)).alias("distinct_ticket_channel_in_actor_or_unknown"),
        F.count(F.lit(1)).over(w_actor_or_unknown).alias("rows_in_actor_or_unknown"),
        F.sum("event_actor_count").over(w_actor_or_unknown).alias("actor_or_unknown_sum_event_actor_count"),
    )
)

ticket_id_stats = (
    with_windows
    .groupBy("ticket_id")
    .agg(F.max("actor_or_unknown_sum_event_actor_count").alias("max_actor_or_unknown_sum_event_actor_count"))
)

above_average = (
    with_windows.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.actor_or_unknown_sum_event_actor_count") == F.col("g.max_actor_or_unknown_sum_event_actor_count"))
    .select(
        "s.*",
        "g.max_actor_or_unknown_sum_event_actor_count",
        F.round(F.col("s.actor_or_unknown_sum_event_actor_count") - F.col("g.max_actor_or_unknown_sum_event_actor_count"), 2).alias("actor_or_unknown_sum_event_actor_count_minus_max"),
    )
)

metrics = (
    above_average
    .select(
        "*",
        F.lead("agent_hired_on", 2).over(w_actor_distinct_ticket_channe).alias("next2_agent_hired_on"),
        F.dense_rank().over(w_all_by_distinct_ticket_chann).alias("distinct_ticket_channel_in_actor_or_unknown_dens"),
        F.count(F.lit(1)).over(w_actor_distinct_ticket_channe_2).alias("running_count"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_type_clean",
        "actor_or_unknown",
        "event_actor_count",
        "ticket_channel",
        "agent_team",
        "distinct_actor_in_actor_or_unknown",
        "event_actor_count_max_3",
        "distinct_ticket_channel_in_actor_or_unknown",
        "rows_in_actor_or_unknown",
        "actor_or_unknown_sum_event_actor_count",
        "max_actor_or_unknown_sum_event_actor_count",
        "actor_or_unknown_sum_event_actor_count_minus_max",
        "next2_agent_hired_on",
        "running_count",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)
