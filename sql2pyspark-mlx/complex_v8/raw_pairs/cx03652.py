from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_to_date_ticket_clos = Window.partitionBy(F.to_date("event_at"), F.month("ticket_closed_at")).orderBy("event_type_len", "id")
w_ticket_id_actor = Window.partitionBy("ticket_id", "actor")
w_event_at_to_date_ticket_clos_2 = Window.partitionBy(F.to_date("event_at"), F.month("ticket_closed_at")).orderBy("event_type_len")

valid = (
    df_ticket_events
    .filter((F.col("event_type") != "assigned") & (F.col("actor") == "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean_2"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

joined = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_clean",
        "s.actor_clean_2",
        "s.event_type_len",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.row_number().over(w_event_at_to_date_ticket_clos).alias("event_type_len_seq"),
        F.size(F.collect_set("event_type").over(w_ticket_id_actor)).alias("distinct_event_type_in_ticket_id_actor"),
        F.max("event_type_len").over(w_ticket_id_actor).alias("ticket_id_actor_max_event_type_len"),
        F.dense_rank().over(w_event_at_to_date_ticket_clos_2).alias("event_type_len_dense_rank"),
    )
)

detail_rows_events = (
    with_trends.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_clean_2",
        "s.ticket_closed_at",
        "s.distinct_event_type_in_ticket_id_actor",
        "s.ticket_id_actor_max_event_type_len",
        "s.event_type_len_dense_rank",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

with_counts = (
    detail_rows_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at_3"),
    )
)

with_counts_2 = (
    with_counts.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at_3",
    )
)

result = (
    with_counts_2
    .cube("event_type", "event_actor")
    .agg(F.max("last_event_at_2").alias("last_last_event_at_2"))
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        F.coalesce(F.col("event_actor"), F.lit("ALL")).alias("event_actor"),
        "last_last_event_at_2",
    )
)
