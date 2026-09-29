from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_opened_at_to_date_act = Window.partitionBy(F.to_date("ticket_opened_at"), "actor").orderBy(F.col("ticket_count").desc(), "id")
w_ticket_count_by_ticket_count = Window.partitionBy("ticket_count").orderBy(F.col("ticket_count_2").desc(), "id")
w_all_by_ticket_opened_at_full = Window.orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev_ticket_count_ticket_age = Window.partitionBy("prev_ticket_count", "ticket_agent_id").orderBy("prev_ticket_count", "id")
w_actor_ticket_id_by_prev_tick = Window.partitionBy("actor", "ticket_id").orderBy(F.col("prev_ticket_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_id_by_prev_tick_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("prev_ticket_count").asc_nulls_last())

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.hour(F.col("event_at")).between(10, 13)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
        F.sum("requester_id").alias("total_ticket_requester_id"),
    )
)

summarized_rows = (
    with_parent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
        "t.total_ticket_requester_id",
    )
)

windowed = (
    summarized_rows
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_opened_at",
        "ticket_agent_id",
        "ticket_count",
        "ticket_count_2",
        F.lag("ticket_count").over(w_ticket_opened_at_to_date_act).alias("prev_ticket_count"),
        F.rank().over(w_ticket_count_by_ticket_count).alias("ticket_count_2_rank"),
        F.nth_value("total_ticket_requester_id", 2).over(w_all_by_ticket_opened_at_full).alias("second_total_ticket_requester_id"),
    )
)

ranked = (
    windowed
    .select(
        "id",
        "actor",
        "ticket_count",
        "ticket_count_2_rank",
        "second_total_ticket_requester_id",
        F.dense_rank().over(w_prev_ticket_count_ticket_age).alias("prev_ticket_count_dense_rank"),
        F.min("prev_ticket_count").over(w_actor_ticket_id_by_prev_tick).alias("min_prev_ticket_count_so_far"),
        F.rank().over(w_actor_ticket_id_by_prev_tick_2).alias("prev_ticket_count_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        F.col("prev_ticket_count_rank").alias("prev_ticket_count_rank_v"),
        F.col("ticket_count_2_rank").alias("ticket_count_2_rank_v"),
    )
    .unpivot(
        ids=["id"],
        values=["prev_ticket_count_rank_v", "ticket_count_2_rank_v"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
