from pyspark.sql import functions as F

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

joined_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        "s.actor_event_type_label",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

derived = (
    joined_ticket
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        F.initcap(F.trim(F.col("ticket_channel"))).alias("ticket_channel_clean"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_agent_id").cast("string")).alias("event_type_ref"),
    )
)

totals = (
    derived
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("ticket_channel") == "phone", 1)).alias("n_ticket_channel_phone"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count(F.when(F.col("ticket_channel").isin("chat", "email", "phone"), 1)).alias("n_ticket_channel_in_chat_email"),
        F.array_sort(F.collect_set("ticket_channel")).alias("ticket_channel_list"),
    )
)

result = (
    totals
    .filter(F.size(F.col("ticket_channel_list")) >= 1)
    .select(
        "ticket_id",
        "n_ticket_channel_phone",
        "distinct_event_types",
        (F.floor(F.col("n_ticket_channel_phone") / 2) * 2).alias("n_ticket_channel_phone_bucket"),
    )
    .orderBy("n_ticket_channel_phone_bucket")
)
