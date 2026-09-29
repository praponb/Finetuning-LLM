from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_closed_at_month_by_ti = Window.partitionBy(F.month("ticket_closed_at")).orderBy("ticket_closed_at", "id")
w_all_by_ticket_closed_at_runn = Window.orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ticket_closed_at = Window.orderBy("ticket_closed_at", "id")
w_ticket_closed_at_month_by_ti_2 = Window.partitionBy(F.month("ticket_closed_at")).orderBy("ticket_closed_at")
w_ticket_channel_by_ticket_clo = Window.partitionBy("ticket_channel").orderBy("ticket_closed_at")
w_ticket_channel_by_ticket_clo_2 = Window.partitionBy("ticket_channel").orderBy("ticket_closed_at", "id")
w_all_by_ticket_closed_at_full = Window.orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_ticket_closed_a = Window.partitionBy("ticket_id").orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_closed_at_month_by_ti_3 = Window.partitionBy(F.month("ticket_closed_at")).orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_closed_at_month_by_ti_4 = Window.partitionBy(F.month("ticket_closed_at")).orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_ticket_closed_a_2 = Window.partitionBy("ticket_id").orderBy(F.col("ticket_closed_at").desc(), "id")

prepared_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("resolved", "replied")) & (F.col("event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

with_details = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor_event_type_label",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.team").alias("agent_team"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.row_number().over(w_ticket_closed_at_month_by_ti).alias("rn"),
        F.first("ticket_closed_at", ignorenulls=True).over(w_all_by_ticket_closed_at_runn).alias("first_known_ticket_closed_at"),
        F.row_number().over(w_all_by_ticket_closed_at).alias("seq_no"),
        F.dense_rank().over(w_ticket_closed_at_month_by_ti_2).alias("ticket_closed_at_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_ticket_closed_at), 3).alias("ticket_closed_at_cume_dist"),
        F.rank().over(w_ticket_channel_by_ticket_clo).alias("ticket_closed_at_rank"),
        F.round(F.percent_rank().over(w_all_by_ticket_closed_at), 3).alias("ticket_closed_at_percent_rank"),
        F.lag("ticket_closed_at").over(w_ticket_channel_by_ticket_clo_2).alias("prev_ticket_closed_at"),
        F.nth_value("ticket_closed_at", 2).over(w_all_by_ticket_closed_at_full).alias("second_ticket_closed_at"),
        F.nth_value("ticket_closed_at", 2).over(w_ticket_id_by_ticket_closed_a).alias("second_ticket_closed_at_2"),
        F.nth_value("ticket_closed_at", 2).over(w_ticket_closed_at_month_by_ti_3).alias("second_ticket_closed_at_3"),
        F.last("ticket_closed_at", ignorenulls=True).over(w_ticket_closed_at_month_by_ti_4).alias("last_known_ticket_closed_at"),
        F.dense_rank().over(w_ticket_id_by_ticket_closed_a_2).alias("ticket_closed_at_dense_rank_2"),
        F.round(F.cume_dist().over(w_ticket_closed_at_month_by_ti), 3).alias("ticket_closed_at_cume_dist_2"),
        F.lag("ticket_closed_at").over(w_ticket_id_by_ticket_closed_a_2).alias("prev_ticket_closed_at_2"),
    )
)

result = (
    sequenced
    .filter(F.col("second_ticket_closed_at") >= F.lit("2025-01-24 01:00:00").cast("timestamp"))
    .select(
        "id",
        "event_type",
        "actor_event_type_label",
        "ticket_channel",
        "ticket_closed_at",
        "first_known_ticket_closed_at",
        "seq_no",
        "ticket_closed_at_dense_rank",
        "ticket_closed_at_rank",
        "ticket_closed_at_percent_rank",
        "prev_ticket_closed_at",
        "second_ticket_closed_at",
        "second_ticket_closed_at_2",
        "second_ticket_closed_at_3",
        "last_known_ticket_closed_at",
        "ticket_closed_at_dense_rank_2",
        "ticket_closed_at_cume_dist_2",
        "prev_ticket_closed_at_2",
        F.abs(F.col("ticket_closed_at_rank") - 5).alias("ticket_closed_at_rank_distance"),
    )
)
