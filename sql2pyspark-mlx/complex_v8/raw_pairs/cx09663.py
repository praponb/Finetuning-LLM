from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_agent_hired_on_runnin = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_ticket_opened = Window.partitionBy("event_type").orderBy(F.col("ticket_opened_at").desc(), "id")
w_ticket_id_ticket_opened_at_m = Window.partitionBy("ticket_id", F.month("ticket_opened_at")).orderBy("agent_hired_on", "id")
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc())
w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_event_type_by_ticket_opened_2 = Window.partitionBy("event_type").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_ticket_id = Window.partitionBy(F.to_date("event_at"), "ticket_id")
w_all_by_ticket_opened_at = Window.orderBy(F.col("ticket_opened_at").desc_nulls_first(), "id")

scoped = df_ticket_events

with_details_ticket = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.channel") == "web")
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_trends = (
    with_details_ticket
    .select(
        "*",
        F.first("event_at", ignorenulls=True).over(w_all_by_agent_hired_on_runnin).alias("first_known_event_at"),
        F.lead("ticket_opened_at").over(w_event_type_by_ticket_opened).alias("next_ticket_opened_at"),
        F.round(F.cume_dist().over(w_ticket_id_ticket_opened_at_m), 3).alias("agent_hired_on_cume_dist"),
        F.round(F.percent_rank().over(w_event_type_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.round(F.cume_dist().over(w_event_type_actor_by_event_at_2), 3).alias("event_at_cume_dist"),
        F.ntile(5).over(w_all_by_agent_hired_on).alias("agent_hired_on_quintile"),
        F.row_number().over(w_event_type_by_ticket_opened).alias("rn"),
        F.lag("event_at").over(w_event_type_by_ticket_opened).alias("prev_event_at"),
        F.nth_value("event_at", 2).over(w_event_type_by_ticket_opened_2).alias("second_event_at"),
    )
)

top_rows = with_trends.filter(F.col("rn") <= 5)

sequenced = (
    top_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_channel",
        "ticket_opened_at",
        "agent_name",
        "agent_hired_on",
        "first_known_event_at",
        "next_ticket_opened_at",
        "agent_hired_on_cume_dist",
        "event_at_cume_dist",
        "agent_hired_on_quintile",
        "second_event_at",
        F.count(F.lit(1)).over(w_event_at_to_date_ticket_id).alias("rows_in_event_at_to_date_ticket_id"),
        F.round(F.cume_dist().over(w_all_by_ticket_opened_at), 3).alias("ticket_opened_at_cume_dist"),
        F.max("event_at_cume_dist").over(w_event_at_to_date_ticket_id).alias("event_at_to_date_ticket_id_max_event_at_cume_dis"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_at",
        "actor",
        "ticket_channel",
        "ticket_opened_at",
        "agent_name",
        "agent_hired_on",
        "first_known_event_at",
        "next_ticket_opened_at",
        "agent_hired_on_cume_dist",
        "event_at_cume_dist",
        "second_event_at",
        "ticket_opened_at_cume_dist",
        F.to_date(F.col("next_ticket_opened_at")).alias("next_ticket_opened_at_date"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)
