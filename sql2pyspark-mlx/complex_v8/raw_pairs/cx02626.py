from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_actor_ticket_id = Window.partitionBy("actor", "ticket_id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_actor = Window.partitionBy("actor")
w_event_at_dense_rank_by_event = Window.partitionBy("event_at_dense_rank").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_dense_rank_by_event_2 = Window.partitionBy("event_at_dense_rank").orderBy("event_at", "id")
w_all_by_event_at_month_share = Window.orderBy("event_at_month_share_of_actor_ticket_id", "id")

recent = (
    df_ticket_events
    .filter((F.col("event_type") != "reopened") & (F.col("ticket_id") <= 39))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

sequenced = (
    recent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_month",
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.round(F.col("event_at_month") / F.nullif(F.sum("event_at_month").over(w_actor_ticket_id), F.lit(0)), 4).alias("event_at_month_share_of_actor_ticket_id"),
        F.lag("event_at_month", 3, 0).over(w_ticket_id_by_event_at).alias("prev3_event_at_month"),
        F.dense_rank().over(w_ticket_id_by_event_at).alias("event_at_dense_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id") >= 2)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_at_month",
        "s.event_at_percent_rank",
        "s.event_at_month_share_of_actor_ticket_id",
        "s.event_at_dense_rank",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.sum("event_at_month").over(w_event_at_dense_rank_by_event).alias("running_event_at_month"),
        F.row_number().over(w_event_at_dense_rank_by_event_2).alias("rn"),
        F.sum("event_at_percent_rank").over(w_event_at_dense_rank_by_event).alias("running_event_at_percent_rank"),
        F.lead("event_at_percent_rank", 2).over(w_all_by_event_at_month_share).alias("next2_event_at_percent_rank"),
        F.round(F.cume_dist().over(w_event_at_dense_rank_by_event_2), 3).alias("event_at_cume_dist"),
    )
)

derived = (
    with_windows
    .filter(F.col("rn") < 8)
    .select(
        "*",
        F.round(F.col("running_event_at_month") / F.nullif(F.col("event_at_percent_rank"), F.lit(0)), 3).alias("running_event_at_month_per_event_at_percent_rank"),
        F.round(F.col("event_at_cume_dist") / F.nullif(F.col("event_at_dense_rank"), F.lit(0)), 3).alias("event_at_cume_dist_per_event_at_dense_rank"),
    )
)

result = (
    derived
    .groupBy("ticket_agent_id", "event_at_dense_rank")
    .pivot("actor", ["system", "customer"])
    .agg(F.sum("event_at_cume_dist_per_event_at_dense_rank"))
)
