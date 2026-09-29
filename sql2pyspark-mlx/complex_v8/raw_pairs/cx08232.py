from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
    )
)

joined_ticket = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

annotated = (
    joined_ticket
    .filter(F.hour(F.col("next_event_at")).between(6, 9))
    .withColumn("event_type_prefix", F.substring(F.trim(F.col("event_type")), 1, 4))
)

result = (
    annotated
    .filter(F.col("event_at_dense_rank").between(3, 7))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "first_event_at",
        "minutes_since_prev_event_at",
        "next_event_at",
        "event_at_cume_dist",
        "event_at_dense_rank",
        "ticket_closed_at",
        "ticket_agent_id",
        "event_type_prefix",
        F.coalesce(F.col("event_at_cume_dist"), F.lit(0.0)).alias("event_at_cume_dist_filled"),
        F.coalesce(F.col("event_at_dense_rank"), F.lit(0)).alias("event_at_dense_rank_filled"),
    )
    .orderBy("event_at_dense_rank")
)
