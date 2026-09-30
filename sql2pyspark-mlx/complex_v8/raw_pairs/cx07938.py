from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id")
w_closed_at_to_date_agent_id_b = Window.partitionBy(F.to_date("closed_at"), "agent_id").orderBy(F.col("csat").desc(), "id")
w_agent_team_by_requester_id_p = Window.partitionBy("agent_team").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_all_by_csat_full = Window.orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_team = Window.partitionBy("agent_team")
w_agent_team_by_requester_id_r = Window.partitionBy("agent_team").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat = Window.partitionBy("csat")
w_agent_team_by_requester_id = Window.partitionBy("agent_team").orderBy(F.col("requester_id").asc_nulls_last(), "id")
w_agent_id_rn_by_last_opened_a = Window.partitionBy("agent_id", "rn").orderBy("last_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_rn_by_last_opened_a_2 = Window.partitionBy("agent_id", "rn").orderBy("last_opened_at")
w_agent_id_rn_by_last_opened_a_3 = Window.partitionBy("agent_id", "rn").orderBy("last_opened_at", "id").rowsBetween(-1, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.col("requester_id") >= 8)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

combined_details_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").between(F.lit("2024-01-30").cast("date"), F.lit("2025-03-30").cast("date"))), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

ranked = (
    combined_details_agent
    .select(
        "*",
        F.row_number().over(w_agent_id_by_requester_id).alias("rn"),
        F.lag("requester_id", 2).over(w_closed_at_to_date_agent_id_b).alias("prev2_requester_id"),
        F.max("csat").over(w_agent_team_by_requester_id_p).alias("csat_max_1_prior"),
        F.nth_value("closed_at", 2).over(w_all_by_csat_full).alias("second_closed_at"),
        F.round(F.percent_rank().over(w_closed_at_to_date_agent_id_b), 3).alias("csat_percent_rank"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_team), 2).alias("requester_id_vs_agent_team_avg"),
        F.round(F.cume_dist().over(w_agent_id_by_requester_id), 3).alias("requester_id_cume_dist"),
        F.last("opened_at").over(w_all_by_csat_full).alias("last_opened_at"),
        F.first("opened_at", ignorenulls=True).over(w_agent_team_by_requester_id_r).alias("first_known_opened_at"),
        F.min("requester_id").over(w_csat).alias("csat_min_requester_id"),
        F.sum("csat").over(w_csat).alias("csat_sum_csat"),
        F.lag("csat").over(w_closed_at_to_date_agent_id_b).alias("prev_csat"),
        F.lead("closed_at", 3).over(w_agent_team_by_requester_id).alias("next3_closed_at"),
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "tags",
        "agent_team",
        "agent_tier",
        "prev2_requester_id",
        "csat_max_1_prior",
        "second_closed_at",
        "csat_percent_rank",
        "requester_id_cume_dist",
        "last_opened_at",
        "first_known_opened_at",
        "csat_min_requester_id",
        "csat_sum_csat",
        "prev_csat",
        "next3_closed_at",
        "distinct_agent_id_in_csat",
        F.count(F.lit(1)).over(w_agent_team).alias("rows_in_agent_team"),
        F.nth_value("prev2_requester_id", 2).over(w_agent_id_rn_by_last_opened_a).alias("second_prev2_requester_id"),
        F.size(F.collect_set("agent_id").over(w_agent_team)).alias("distinct_agent_id_in_agent_team"),
        F.round(F.cume_dist().over(w_agent_id_rn_by_last_opened_a_2), 3).alias("last_opened_at_cume_dist"),
        F.round(F.avg("csat").over(w_agent_id_rn_by_last_opened_a_3), 2).alias("csat_avg_2"),
    )
)

result = (
    with_windows
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "agent_team",
        "agent_tier",
        "prev2_requester_id",
        "csat_max_1_prior",
        "csat_percent_rank",
        "requester_id_cume_dist",
        "last_opened_at",
        "first_known_opened_at",
        "csat_min_requester_id",
        "csat_sum_csat",
        "prev_csat",
        "next3_closed_at",
        "second_prev2_requester_id",
        "csat_avg_2",
        F.lower(F.regexp_replace(F.trim(F.col("agent_team")), " ", "_")).alias("agent_team_slug"),
        F.dayofweek(F.col("first_known_opened_at")).alias("first_known_opened_at_dayofweek"),
    )
    .orderBy(F.col("agent_tier").asc_nulls_last())
)
