from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_year_by_csat = Window.partitionBy(F.year("closed_at")).orderBy(F.col("csat").desc(), "id")
w_channel = Window.partitionBy("channel")
w_agent_hired_on_year_opened_a = Window.partitionBy(F.year("agent_hired_on"), F.year("opened_at")).orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_hired_on_year_opened_a_2 = Window.partitionBy(F.year("agent_hired_on"), F.year("opened_at")).orderBy(F.col("csat").desc(), "id")
w_agent_hired_on_year_opened_a_3 = Window.partitionBy(F.year("agent_hired_on"), F.year("opened_at")).orderBy(F.col("csat").desc(), "id").rowsBetween(-2, Window.currentRow)
w_agent_hired_on_year_opened_a_4 = Window.partitionBy(F.year("agent_hired_on"), F.year("opened_at")).orderBy(F.col("csat").desc())
w_csat_agent_team_by_opened_at = Window.partitionBy("csat", "agent_team").orderBy(F.col("opened_at").desc(), "id")
w_csat_agent_team_by_opened_at_2 = Window.partitionBy("csat", "agent_team").orderBy(F.col("opened_at").desc())
w_closed_at_year_by_csat_prior = Window.partitionBy(F.year("closed_at")).orderBy(F.col("csat").desc(), "id").rowsBetween(-3, -1)
w_closed_at_year_by_csat_runni = Window.partitionBy(F.year("closed_at")).orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter(F.col("csat").between(2, 4))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

enriched = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "agent_team",
        "agent_hired_on",
        F.lag("agent_hired_on").over(w_closed_at_year_by_csat).alias("prev_agent_hired_on"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.nth_value("closed_at", 2).over(w_agent_hired_on_year_opened_a).alias("second_closed_at"),
        F.lag("closed_at", 2).over(w_agent_hired_on_year_opened_a_2).alias("prev2_closed_at"),
        F.size(F.collect_set("agent_team").over(w_channel)).alias("distinct_agent_team_in_channel"),
        F.sum("csat").over(w_agent_hired_on_year_opened_a_3).alias("csat_sum_3"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_agent_hired_on_year_opened_a_2)).alias("days_since_prev_agent_hired_on"),
        F.rank().over(w_agent_hired_on_year_opened_a_4).alias("csat_rank"),
        F.lag("csat", 3, 0).over(w_csat_agent_team_by_opened_at).alias("prev3_csat"),
        F.dense_rank().over(w_csat_agent_team_by_opened_at_2).alias("opened_at_dense_rank"),
        F.max("csat").over(w_closed_at_year_by_csat_prior).alias("csat_max_3_prior"),
        F.rank().over(w_csat_agent_team_by_opened_at_2).alias("opened_at_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_channel), 2).alias("csat_vs_channel_avg"),
        F.max("csat").over(w_closed_at_year_by_csat_runni).alias("max_csat_so_far"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "agent_team",
        "agent_hired_on",
        "prev_agent_hired_on",
        "second_closed_at",
        "prev2_closed_at",
        "distinct_agent_team_in_channel",
        "csat_sum_3",
        "csat_rank",
        "prev3_csat",
        "opened_at_dense_rank",
        "csat_max_3_prior",
        "opened_at_rank",
        "csat_vs_channel_avg",
        "max_csat_so_far",
    )
    .orderBy(F.col("csat_rank").desc())
)
