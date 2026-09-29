from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_csat_moving = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").desc(), "id").rowsBetween(-1, Window.currentRow)
w_agent_id_csat_by_csat_ties = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").desc())
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_tier_by_closed_at_prio = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc(), "id").rowsBetween(-6, -1)
w_agent_tier = Window.partitionBy("agent_tier")
w_agent_tier_by_closed_at_ties = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc())
w_agent_tier_by_closed_at = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc(), "id")
w_agent_id_by_closed_at_prior = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(-2, -1)
w_agent_tier_by_closed_at_full = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_csat_by_csat_runnin = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
    )
)

combined_details_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.least_csat_requester_id",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    combined_details_agent
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        "least_csat_requester_id",
        "agent_name",
        "agent_hired_on",
        F.sum("least_csat_requester_id").over(w_agent_id_csat_by_csat_moving).alias("least_csat_requester_id_sum_2"),
        F.rank().over(w_agent_id_csat_by_csat_ties).alias("csat_rank"),
        F.lead("least_csat_requester_id", 2).over(w_agent_id_by_closed_at).alias("next2_least_csat_requester_id"),
        F.sum("csat").over(w_agent_tier_by_closed_at_prio).alias("csat_sum_6_prior"),
        F.lead("closed_at", 2).over(w_agent_id_by_closed_at).alias("next2_closed_at"),
        F.round(F.avg("least_csat_requester_id").over(w_agent_tier), 2).alias("agent_tier_avg_least_csat_requester_id"),
        F.dense_rank().over(w_agent_tier_by_closed_at_ties).alias("closed_at_dense_rank"),
        F.lag("least_csat_requester_id").over(w_agent_tier_by_closed_at).alias("prev_least_csat_requester_id"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_tier), 2).alias("csat_vs_agent_tier_avg"),
        F.sum("csat").over(w_agent_tier).alias("agent_tier_sum_csat"),
        F.max("csat").over(w_agent_id_by_closed_at_prior).alias("csat_max_2_prior"),
        F.nth_value("agent_hired_on", 2).over(w_agent_tier_by_closed_at_full).alias("second_agent_hired_on"),
        F.first("closed_at", ignorenulls=True).over(w_agent_id_csat_by_csat_runnin).alias("first_known_closed_at"),
    )
)

result = (
    metrics
    .filter(F.col("csat_sum_6_prior") <= 12)
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "least_csat_requester_id",
        "agent_name",
        "agent_hired_on",
        "least_csat_requester_id_sum_2",
        "next2_least_csat_requester_id",
        "next2_closed_at",
        "agent_tier_avg_least_csat_requester_id",
        "prev_least_csat_requester_id",
        "csat_vs_agent_tier_avg",
        "agent_tier_sum_csat",
        "csat_max_2_prior",
        "second_agent_hired_on",
        "first_known_closed_at",
        (F.floor(F.col("agent_tier_sum_csat") / 10) * 10).alias("agent_tier_sum_csat_bucket"),
    )
    .orderBy(
        F.col("agent_tier_sum_csat").desc_nulls_first(),
        F.col("next2_closed_at").desc(),
    )
)
