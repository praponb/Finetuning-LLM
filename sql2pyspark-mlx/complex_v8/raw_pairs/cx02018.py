from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_month_by_even = Window.partitionBy("actor", F.month("event_at")).orderBy("event_at", "id")
w_actor_event_at_month_by_even_2 = Window.partitionBy("actor", F.month("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_month_by_even_3 = Window.partitionBy("actor", F.month("event_at")).orderBy("event_at")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").desc())
w_actor_event_at_month = Window.partitionBy("actor", F.month("event_at"))
w_all_by_event_at_dense_rank_p = Window.orderBy(F.col("event_at_dense_rank").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_ticket_id_event_at_dense_ran = Window.partitionBy("ticket_id", "event_at_dense_rank").orderBy("agent_hired_on")
w_ticket_id_event_at_dense_ran_2 = Window.partitionBy("ticket_id", "event_at_dense_rank").orderBy("agent_hired_on", "id").rowsBetween(-3, Window.currentRow)
w_ticket_id_event_at_dense_ran_3 = Window.partitionBy("ticket_id", "event_at_dense_rank").orderBy("agent_hired_on", "id")

valid = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 23)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.nth_value("agent_hired_on", 2).over(w_actor_by_event_at_full).alias("second_agent_hired_on"),
        F.dense_rank().over(w_actor_ticket_id_by_event_at).alias("event_at_dense_rank"),
        F.first("event_at").over(w_actor_ticket_id_by_event_at_2).alias("first_event_at"),
        F.nth_value("agent_hired_on", 2).over(w_all_by_event_at_full).alias("second_agent_hired_on_2"),
        F.lag("event_at", 3).over(w_actor_event_at_month_by_even).alias("prev3_event_at"),
        F.last("agent_hired_on").over(w_actor_event_at_month_by_even_2).alias("last_agent_hired_on"),
        F.dense_rank().over(w_actor_event_at_month_by_even_3).alias("event_at_dense_rank_2"),
        F.round(F.percent_rank().over(w_actor_by_event_at_ties), 3).alias("event_at_percent_rank_2"),
        F.lag("agent_hired_on", 2).over(w_actor_event_at_month_by_even).alias("prev2_agent_hired_on"),
    )
)

leaders = sequenced.filter(F.col("event_at_dense_rank_2") <= 2)

metrics = (
    leaders
    .select(
        "*",
        F.size(F.collect_set("ticket_agent_id").over(w_actor_event_at_month)).alias("distinct_ticket_agent_id_in_actor_event_at_month"),
        F.max("event_at_dense_rank").over(w_all_by_event_at_dense_rank_p).alias("event_at_dense_rank_max_3_prior"),
        F.rank().over(w_ticket_id_event_at_dense_ran).alias("agent_hired_on_rank"),
        F.round(F.col("event_at_percent_rank") - F.avg("event_at_percent_rank").over(w_actor_event_at_month), 2).alias("event_at_percent_rank_vs_actor_event_at_month_av"),
        F.max("event_at_dense_rank_2").over(w_ticket_id_event_at_dense_ran_2).alias("event_at_dense_rank_2_max_4"),
        F.lead("prev2_agent_hired_on").over(w_ticket_id_event_at_dense_ran_3).alias("next_prev2_agent_hired_on"),
    )
)

result = (
    metrics
    .select(
        "id",
        "event_type",
        "agent_hired_on",
        "event_at_percent_rank",
        "second_agent_hired_on",
        "event_at_dense_rank",
        "first_event_at",
        "second_agent_hired_on_2",
        "prev3_event_at",
        "event_at_dense_rank_2",
        "event_at_percent_rank_2",
        "distinct_ticket_agent_id_in_actor_event_at_month",
        "event_at_dense_rank_max_3_prior",
        "event_at_percent_rank_vs_actor_event_at_month_av",
        "event_at_dense_rank_2_max_4",
        "next_prev2_agent_hired_on",
        F.when(F.col("event_at_dense_rank") >= 6, "HIGH").when(F.col("event_at_dense_rank") >= 4, "MEDIUM").otherwise("LOW").alias("event_at_dense_rank_tier"),
    )
    .orderBy("next_prev2_agent_hired_on", F.col("agent_hired_on").desc_nulls_first())
)
