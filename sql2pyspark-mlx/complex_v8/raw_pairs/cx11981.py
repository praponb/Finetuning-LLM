from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").asc_nulls_last(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at_2 = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_2 = Window.orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_full_2 = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_agent_id_by_second_ev = Window.partitionBy("ticket_agent_id").orderBy("second_event_at")
w_ticket_channel_by_second_eve = Window.partitionBy("ticket_channel").orderBy(F.col("second_event_at").desc())
w_last_known_event_at_year_age = Window.partitionBy(F.year("last_known_event_at"), "agent_team")
w_ticket_agent_id_event_at_qua = Window.partitionBy("ticket_agent_id", "event_at_quartile").orderBy("second_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("resolved", "replied", "escalated")) | (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

combined_details_ticket = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_clean",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.team").alias("agent_team"),
    )
)

metrics = (
    combined_details_ticket
    .select(
        "*",
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.lead("event_at", 2).over(w_all_by_event_at).alias("next2_event_at"),
        F.lag("event_at", 3).over(w_ticket_id_by_event_at).alias("prev3_event_at"),
        F.ntile(4).over(w_ticket_id_by_event_at_2).alias("event_at_quartile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(10).over(w_ticket_id_by_event_at).alias("event_at_decile"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full_2).alias("second_event_at"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("last_known_event_at"),
    )
)

labeled = (
    metrics
    .select(
        "id",
        "event_at",
        "event_type",
        "actor_clean",
        "ticket_channel",
        "ticket_agent_id",
        "agent_team",
        "next2_event_at",
        "prev3_event_at",
        "event_at_quartile",
        "event_at_cume_dist",
        "event_at_cume_dist_2",
        "second_event_at",
        "first_known_event_at",
        "event_at_percent_rank",
        "last_known_event_at",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.concat(F.col("ticket_channel"), F.lit("#"), F.col("id").cast("string")).alias("ticket_channel_ref"),
    )
)

windowed = (
    labeled
    .select(
        "id",
        "actor_clean",
        "ticket_channel",
        "ticket_agent_id",
        "agent_team",
        "next2_event_at",
        "event_at_cume_dist",
        "event_at_cume_dist_2",
        "second_event_at",
        "first_known_event_at",
        "event_at_percent_rank",
        "last_known_event_at",
        "event_type_ref",
        F.rank().over(w_ticket_agent_id_by_second_ev).alias("second_event_at_rank"),
        F.round(F.cume_dist().over(w_ticket_channel_by_second_eve), 3).alias("second_event_at_cume_dist"),
        F.count(F.lit(1)).over(w_last_known_event_at_year_age).alias("rows_in_last_known_event_at_year_agent_team"),
        F.first("event_at", ignorenulls=True).over(w_ticket_agent_id_event_at_qua).alias("first_known_event_at_2"),
        F.last("event_at_cume_dist", ignorenulls=True).over(w_ticket_agent_id_event_at_qua).alias("last_known_event_at_cume_dist"),
        F.round(F.col("event_at_quartile") / F.nullif(F.sum("event_at_quartile").over(w_last_known_event_at_year_age), F.lit(0)), 4).alias("event_at_quartile_share_of_last_known_event_at_y"),
    )
)

result = (
    windowed
    .filter(F.col("last_known_event_at_cume_dist") < 0.6)
    .select(
        "id",
        "actor_clean",
        "agent_team",
        "next2_event_at",
        "event_at_cume_dist",
        "event_at_cume_dist_2",
        "first_known_event_at",
        "last_known_event_at",
        "event_type_ref",
        "second_event_at_rank",
        "rows_in_last_known_event_at_year_agent_team",
        "first_known_event_at_2",
        "last_known_event_at_cume_dist",
        "event_at_quartile_share_of_last_known_event_at_y",
    )
    .orderBy(F.col("next2_event_at").asc_nulls_last(), "id")
    .limit(20)
)
