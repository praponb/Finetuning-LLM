from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_by_closed_at = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc(), "id")
w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())
w_agent_team_agent_tier_by_clo = Window.partitionBy("agent_team", "agent_tier").orderBy("closed_at", "id")
w_agent_tier_by_closed_at_runn = Window.partitionBy("agent_tier").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_agent_team_by_opene = Window.partitionBy("agent_id", "agent_team").orderBy("opened_at", "id")
w_all_by_agent_hired_on_ties = Window.orderBy("agent_hired_on")
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")

cleaned_tickets = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        F.when(F.col("requester_id") >= 18, "A").when(F.col("requester_id") >= 7, "B").otherwise("C").alias("requester_id_tier"),
    )
)

enriched_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    enriched_agent
    .select(
        "*",
        F.round(F.cume_dist().over(w_agent_tier_by_closed_at), 3).alias("closed_at_cume_dist"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_all_by_agent_hired_on))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.dense_rank().over(w_all_by_opened_at_ties).alias("opened_at_dense_rank"),
        F.dense_rank().over(w_agent_team_agent_tier_by_clo).alias("closed_at_dense_rank"),
        F.first("opened_at", ignorenulls=True).over(w_agent_tier_by_closed_at_runn).alias("first_known_opened_at"),
        F.lag("closed_at").over(w_agent_id_agent_team_by_opene).alias("prev_closed_at"),
        F.round(F.cume_dist().over(w_agent_team_agent_tier_by_clo), 3).alias("closed_at_cume_dist_2"),
        F.round(F.cume_dist().over(w_all_by_agent_hired_on_ties), 3).alias("agent_hired_on_cume_dist"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_all_by_closed_at)).alias("days_since_prev_agent_hired_on"),
        F.lag("closed_at", 2).over(w_all_by_agent_hired_on).alias("prev2_closed_at"),
        F.lead("agent_hired_on").over(w_all_by_opened_at).alias("next_agent_hired_on"),
        F.round(F.percent_rank().over(w_all_by_agent_hired_on), 3).alias("agent_hired_on_percent_rank"),
        F.lag("agent_hired_on", 2).over(w_all_by_opened_at).alias("prev2_agent_hired_on"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_opened_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
    )
)

result = (
    windowed
    .filter(F.hour(F.col("opened_at")).between(9, 13))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "agent_tier",
        "closed_at_cume_dist",
        "minutes_since_prev_opened_at",
        "closed_at_dense_rank",
        "prev_closed_at",
        "closed_at_cume_dist_2",
        "agent_hired_on_cume_dist",
        "prev2_closed_at",
        "next_agent_hired_on",
        "agent_hired_on_percent_rank",
        "prev2_agent_hired_on",
        "minutes_since_prev_closed_at",
        F.concat(F.col("agent_team"), F.lit("#"), F.col("agent_id").cast("string")).alias("agent_team_ref"),
        F.greatest(F.col("agent_hired_on_cume_dist"), F.col("opened_at_dense_rank")).alias("greatest_agent_hired_on_cume_dist_opened_at_dens"),
    )
    .orderBy(F.col("opened_at").desc(), "agent_team_ref", "id")
    .limit(10)
)
