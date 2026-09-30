from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_agent_id = Window.partitionBy("ticket_agent_id")
w_event_type_event_type_len_by = Window.partitionBy("event_type", "event_type_len").orderBy("event_type_len", "id").rowsBetween(-6, Window.currentRow)
w_event_type_len_actor_len_by = Window.partitionBy("event_type_len", "actor_len").orderBy(F.col("actor_len").desc(), "id")
w_event_type_event_type_len_by_2 = Window.partitionBy("event_type", "event_type_len").orderBy("event_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_event_type_len_by_3 = Window.partitionBy("event_type", "event_type_len").orderBy("event_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_len_actor_len_by_2 = Window.partitionBy("event_type_len", "actor_len").orderBy(F.col("actor_len").desc())

prepared = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-08 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

tagged = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "ticket_agent_id",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_agent_id",
        "actor_len",
        "event_type_len",
        F.round(F.col("event_type_len") - F.avg("event_type_len").over(w_ticket_agent_id), 2).alias("event_type_len_vs_ticket_agent_id_avg"),
        F.round(F.avg("actor_len").over(w_event_type_event_type_len_by), 2).alias("actor_len_avg_7"),
        F.row_number().over(w_event_type_len_actor_len_by).alias("rn"),
        F.first("event_type_len", ignorenulls=True).over(w_event_type_event_type_len_by_2).alias("first_known_event_type_len"),
        F.last("actor_len").over(w_event_type_event_type_len_by_3).alias("last_actor_len"),
        F.round(F.cume_dist().over(w_event_type_len_actor_len_by_2), 3).alias("actor_len_cume_dist"),
        F.rank().over(w_event_type_len_actor_len_by_2).alias("actor_len_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_agent_id",
        "event_type_len",
        "event_type_len_vs_ticket_agent_id_avg",
        "rn",
        "first_known_event_type_len",
        "last_actor_len",
        "actor_len_cume_dist",
        "actor_len_rank",
        F.round(F.col("actor_len_cume_dist") * 1.07, 2).alias("actor_len_cume_dist_adj"),
    )
    .orderBy(F.col("event_type_len").desc())
)
