from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_running = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev2_event_at_month_event_a = Window.partitionBy(F.month("prev2_event_at"), "event_at_dense_rank").orderBy(F.col("prev2_event_at").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_prev2_event_at_month_event_a_2 = Window.partitionBy(F.month("prev2_event_at"), "event_at_dense_rank").orderBy(F.col("prev2_event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_dense_rank_event_ty = Window.partitionBy("event_at_dense_rank", "event_type").orderBy("first_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_known_event_at_to_date = Window.partitionBy(F.to_date("last_known_event_at"), F.year("last_known_event_at_2"))
w_last_known_event_at_year_by = Window.partitionBy(F.year("last_known_event_at")).orderBy("event_at_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) | (F.hour(F.col("event_at")).between(6, 10)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

with_details_ticket = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.team").alias("agent_team"),
    )
)

with_trends = (
    with_details_ticket
    .select(
        "*",
        F.last("event_at", ignorenulls=True).over(w_actor_by_event_at_running).alias("last_known_event_at"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.lag("event_at", 2).over(w_all_by_event_at).alias("prev2_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_event_type_by_even).alias("last_known_event_at_2"),
        F.lead("event_at").over(w_ticket_id_event_type_by_even_2).alias("next_event_at"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.first("event_at").over(w_actor_by_event_at_full).alias("first_event_at"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_label",
        "ticket_tags",
        "agent_team",
        "last_known_event_at",
        "prev_event_at",
        "prev2_event_at",
        "last_known_event_at_2",
        "next_event_at",
        "event_at_dense_rank",
        F.max("event_at_cume_dist").over(w_prev2_event_at_month_event_a).alias("event_at_cume_dist_max_4"),
        F.last("event_at_cume_dist", ignorenulls=True).over(w_prev2_event_at_month_event_a_2).alias("last_known_event_at_cume_dist"),
        F.first("prev_event_at", ignorenulls=True).over(w_event_at_dense_rank_event_ty).alias("first_known_prev_event_at"),
        F.count(F.lit(1)).over(w_last_known_event_at_to_date).alias("rows_in_last_known_event_at_to_date_last_known_e"),
        F.last("event_at_cume_dist").over(w_last_known_event_at_year_by).alias("last_event_at_cume_dist"),
        F.round(F.avg("event_at_tercile").over(w_last_known_event_at_to_date), 2).alias("last_known_event_at_to_date_last_known_event_at_"),
    )
)

result = (
    metrics
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_label",
        "ticket_tags",
        "agent_team",
        "last_known_event_at",
        "prev2_event_at",
        "last_known_event_at_2",
        "next_event_at",
        "event_at_dense_rank",
        "last_known_event_at_cume_dist",
        "first_known_prev_event_at",
        "last_event_at_cume_dist",
    )
    .orderBy(
        F.col("last_known_event_at").asc_nulls_last(),
        F.col("last_event_at_cume_dist").desc(),
        "id",
    )
    .limit(20)
)
