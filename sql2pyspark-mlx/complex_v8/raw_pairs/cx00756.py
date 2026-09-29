from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_channel_len_by_open = Window.partitionBy("agent_id", "channel_len").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_csat_by_csat_running = Window.partitionBy("csat").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.col("agent_id") <= 8)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
        F.when(F.col("requester_id") >= 18, "A").when(F.col("requester_id") >= 7, "B").otherwise("C").alias("requester_id_tier"),
    )
)

annotated = (
    filtered
    .filter(~F.col("requester_id_tier").isin("C", "A"))
    .select(
        "*",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd HH:00").alias("opened_at_label"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label_2"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "channel_prefix",
        "channel_ref",
        "requester_id_tier",
        "opened_at_label_2",
        "channel_len",
        F.ntile(5).over(w_agent_id_channel_len_by_open).alias("opened_at_quintile"),
        F.size(F.collect_set("requester_id_tier").over(w_agent_id_channel)).alias("distinct_requester_id_tier_in_agent_id_channel"),
        F.min("requester_id").over(w_csat_by_csat_running).alias("min_requester_id_so_far"),
    )
)

agent_id_stats = (
    ranked
    .groupBy("agent_id")
    .agg(F.percentile_approx("opened_at_quintile", 0.5).alias("median_opened_at_quintile"))
)

above_average = (
    ranked.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.opened_at_quintile") >= F.col("g.median_opened_at_quintile"))
    .select(
        "s.*",
        "g.median_opened_at_quintile",
        F.round(F.col("s.opened_at_quintile") - F.col("g.median_opened_at_quintile"), 2).alias("opened_at_quintile_minus_median"),
    )
)

line_level_events = (
    above_average.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (~F.col("t.actor").isin("agent", "customer")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.channel_prefix",
        "s.channel_ref",
        "s.requester_id_tier",
        "s.opened_at_label_2",
        "s.opened_at_quintile",
        "s.distinct_requester_id_tier_in_agent_id_channel",
        "s.min_requester_id_so_far",
        "s.median_opened_at_quintile",
        "s.opened_at_quintile_minus_median",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    line_level_events
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "channel_prefix",
        "requester_id_tier",
        "opened_at_quintile",
        "distinct_requester_id_tier_in_agent_id_channel",
        "min_requester_id_so_far",
        "median_opened_at_quintile",
        "opened_at_quintile_minus_median",
        "event_id",
        "event_actor",
        F.when(F.col("requester_id_tier").isin("A", "C"), "group_a").when(F.col("requester_id_tier") == "B", "group_b").otherwise("other").alias("requester_id_tier_group"),
        F.lower(F.regexp_replace(F.trim(F.col("event_actor")), " ", "_")).alias("event_actor_slug"),
    )
    .orderBy(F.col("id").desc(), "event_id")
    .limit(10)
)
