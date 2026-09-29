from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), F.col("id").desc())

recent_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_type") == "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
    )
)

latest_per_key = (
    recent_events
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

qualified = (
    latest_per_key.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") == "P2"), "left_semi")
)

with_parent_ticket = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

qualified_2 = (
    with_parent_ticket.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.ticket_agent_id")) & (F.col("t.priority") == "P4") & (F.col("t.channel").isNotNull()), "left_anti")
    .filter(~F.col("event_type").isin("assigned", "replied"))
)

derived = (
    qualified_2
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_label",
        "ticket_agent_id",
        "ticket_opened_at",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        (F.col("ticket_opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("ticket_opened_at_plus_30m"),
        F.date_format(F.col("ticket_opened_at"), "yyyy-MM-dd HH:00").alias("ticket_opened_at_label"),
    )
)

result = (
    derived
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_label",
        "ticket_agent_id",
        "ticket_opened_at",
        "event_type_len",
        "ticket_opened_at_plus_30m",
        "ticket_opened_at_label",
        F.dayofweek(F.col("ticket_opened_at")).alias("ticket_opened_at_dayofweek"),
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
    .orderBy("ticket_opened_at", F.col("event_type_prefix").desc(), "id")
    .limit(20)
)
