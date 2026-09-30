from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_moving_1 = Window.orderBy("event_at", "id").rowsBetween(-1, Window.currentRow)
w_actor_group_event_type = Window.partitionBy("actor_group", "event_type")
w_actor_group_ticket_priority = Window.partitionBy("actor_group", "ticket_priority")
w_prev2_event_at_year_by_event = Window.partitionBy("prev2_event_at_year").orderBy(F.col("event_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.col("ticket_id") >= 48))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.year(F.col("event_at")).alias("event_at_year"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

ranked = (
    cleaned_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_group",
        "event_at_year",
        F.lag("event_at_year", 2).over(w_all_by_event_at).alias("prev2_event_at_year"),
        F.round(F.avg("event_at_year").over(w_all_by_event_at_moving_1), 2).alias("event_at_year_avg_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.min("event_at_year").over(w_actor_group_event_type).alias("actor_group_event_type_min_event_at_year"),
    )
)

with_details_ticket = (
    ranked.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

with_trends = (
    with_details_ticket
    .select(
        "*",
        F.round(F.col("event_at_year_avg_2") - F.avg("event_at_year_avg_2").over(w_actor_group_ticket_priority), 2).alias("event_at_year_avg_2_vs_actor_group_ticket_priori"),
        F.round(F.col("event_at_seq") - F.avg("event_at_seq").over(w_actor_group_ticket_priority), 2).alias("event_at_seq_vs_actor_group_ticket_priority_avg"),
        F.last("event_at_year_avg_2", ignorenulls=True).over(w_prev2_event_at_year_by_event).alias("last_known_event_at_year_avg_2"),
    )
)

result = (
    with_trends
    .cube("event_at_seq", "prev2_event_at_year", "actor")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("ticket_priority") == "P3", F.col("event_at_year_avg_2_vs_actor_group_ticket_priori"))).alias("event_at_year_avg_2_vs_actor_group_ticket_priori_2"),
        F.round(F.avg("prev2_event_at_year"), 2).alias("avg_prev2_event_at_year"),
        F.sum("actor_group_event_type_min_event_at_year").alias("total_actor_group_event_type_min_event_at_year"),
    )
    .orderBy(
        F.col("event_at_seq").asc_nulls_last(),
        F.col("prev2_event_at_year").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
