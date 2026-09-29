from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_agent_team_csat_distance_by = Window.partitionBy("agent_team", "csat_distance").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_agent_team = Window.partitionBy("agent_team")
w_agent_id_closed_at_to_date = Window.partitionBy("agent_id", F.to_date("closed_at"))
w_all_by_csat_distance = Window.orderBy(F.col("csat_distance").desc(), "id")
w_agent_id_agent_hired_on_mont = Window.partitionBy("agent_id", F.date_trunc("month", "agent_hired_on")).orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev_csat_distance_prev_csat = Window.partitionBy("prev_csat_distance", "prev_csat_distance_2")
w_all_by_first_agent_hired_on = Window.orderBy("first_agent_hired_on", "id")
w_all_by_first_agent_hired_on_2 = Window.orderBy("first_agent_hired_on", "id").rowsBetween(-6, Window.currentRow)
w_next2_csat_distance_by_agent = Window.partitionBy("next2_csat_distance").orderBy(F.col("agent_hired_on").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.abs(F.col("csat") - 4).alias("csat_distance"),
    )
)

joined_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.csat_distance",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    joined_agent
    .select(
        "*",
        F.lead("csat_distance", 2, 0).over(w_all_by_requester_id).alias("next2_csat_distance"),
        F.nth_value("agent_hired_on", 2).over(w_agent_team_csat_distance_by).alias("second_agent_hired_on"),
        F.dense_rank().over(w_all_by_requester_id_ties).alias("requester_id_dense_rank"),
        F.lag("csat_distance", 1, 0).over(w_all_by_requester_id).alias("prev_csat_distance"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_team), 2).alias("requester_id_vs_agent_team_avg"),
        F.min("csat_distance").over(w_agent_id_closed_at_to_date).alias("agent_id_closed_at_to_date_min_csat_distance"),
        F.lag("csat_distance", 1, 0).over(w_all_by_csat_distance).alias("prev_csat_distance_2"),
        F.first("agent_hired_on").over(w_agent_id_agent_hired_on_mont).alias("first_agent_hired_on"),
        F.count(F.lit(1)).over(w_agent_team).alias("rows_in_agent_team"),
        F.max("csat_distance").over(w_agent_id_closed_at_to_date).alias("agent_id_closed_at_to_date_max_csat_distance"),
        F.round(F.percent_rank().over(w_all_by_csat_distance), 3).alias("csat_distance_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_requester_id), 3).alias("requester_id_percent_rank"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.col("next2_csat_distance") - F.avg("next2_csat_distance").over(w_prev_csat_distance_prev_csat), 2).alias("next2_csat_distance_vs_prev_csat_distance_prev_c"),
        F.rank().over(w_all_by_first_agent_hired_on).alias("first_agent_hired_on_rank"),
        F.sum("requester_id_percent_rank").over(w_all_by_first_agent_hired_on_2).alias("requester_id_percent_rank_sum_7"),
        F.sum("csat_distance_percent_rank").over(w_next2_csat_distance_by_agent).alias("csat_distance_percent_rank_sum_4"),
    )
)

result = (
    sequenced_2
    .filter(F.col("csat_distance_percent_rank_sum_4").between(0.3, 2.5))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "tags",
        "csat_distance",
        "next2_csat_distance",
        "second_agent_hired_on",
        "requester_id_dense_rank",
        "agent_id_closed_at_to_date_min_csat_distance",
        "first_agent_hired_on",
        "first_agent_hired_on_rank",
        "requester_id_percent_rank_sum_7",
        "csat_distance_percent_rank_sum_4",
    )
    .orderBy(
        "requester_id",
        F.col("requester_id_percent_rank_sum_7").desc_nulls_first(),
    )
)
