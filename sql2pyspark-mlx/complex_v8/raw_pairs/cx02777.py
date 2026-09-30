from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_or_unknown_by_event_at = Window.partitionBy("actor_or_unknown").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_at_month_by_ticket_ope = Window.partitionBy(F.month("event_at")).orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_ticket_ope_2 = Window.partitionBy(F.month("event_at")).orderBy("ticket_opened_at", "id")
w_actor_or_unknown_by_event_at_2 = Window.partitionBy("actor_or_unknown").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_actor_or_unknown_by_event_at_3 = Window.partitionBy("actor_or_unknown").orderBy(F.col("event_at").asc_nulls_last())

cleaned = (
    df_ticket_events
    .filter(F.col("event_type") != "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

combined_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

with_trends = (
    combined_details_ticket
    .select(
        "*",
        F.nth_value("ticket_opened_at", 2).over(w_ticket_id_by_event_at_full).alias("second_ticket_opened_at"),
        F.lead("ticket_opened_at").over(w_actor_or_unknown_by_event_at).alias("next_ticket_opened_at"),
        F.lag("ticket_opened_at").over(w_ticket_id_by_event_at).alias("prev_ticket_opened_at"),
        F.last("ticket_opened_at").over(w_event_at_month_by_ticket_ope).alias("last_ticket_opened_at"),
        F.row_number().over(w_event_at_month_by_ticket_ope_2).alias("ticket_opened_at_seq"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.first("event_at").over(w_actor_or_unknown_by_event_at_2).alias("first_event_at"),
        F.dense_rank().over(w_ticket_id_by_event_at).alias("event_at_dense_rank"),
        F.dense_rank().over(w_ticket_id_by_event_at_ties).alias("event_at_dense_rank_2"),
        F.rank().over(w_ticket_id_by_event_at).alias("event_at_rank"),
        F.round((F.unix_timestamp("ticket_opened_at") - F.unix_timestamp(F.lag("ticket_opened_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_ticket_opened_at"),
        F.rank().over(w_actor_or_unknown_by_event_at_3).alias("event_at_rank_2"),
    )
)

annotated = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_opened_at",
        "ticket_channel",
        "second_ticket_opened_at",
        "next_ticket_opened_at",
        "last_ticket_opened_at",
        "ticket_opened_at_seq",
        "event_at_cume_dist",
        "first_event_at",
        "event_at_dense_rank",
        "event_at_rank",
        "minutes_since_prev_ticket_opened_at",
        "event_at_rank_2",
        F.round(F.col("event_at_cume_dist") * 1.1, 2).alias("event_at_cume_dist_adj"),
    )
)

result = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_channel",
        "next_ticket_opened_at",
        "last_ticket_opened_at",
        "first_event_at",
        "event_at_dense_rank",
        "minutes_since_prev_ticket_opened_at",
        "event_at_cume_dist_adj",
        F.substring(F.trim(F.col("ticket_channel")), 1, 2).alias("ticket_channel_prefix"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)
