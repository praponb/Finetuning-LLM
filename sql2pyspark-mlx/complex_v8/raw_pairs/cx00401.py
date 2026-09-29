from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_or_unknown_by_first_tic = Window.partitionBy("tier_or_unknown").orderBy(F.col("first_ticket_closed_at").desc(), "id")
w_tier_or_unknown_by_first_tic_2 = Window.partitionBy("tier_or_unknown").orderBy("first_ticket_closed_at_2", "id")
w_tier_tier_or_unknown_by_firs = Window.partitionBy("tier", "tier_or_unknown").orderBy(F.col("first_ticket_closed_at_2").desc(), "id")
w_first_ticket_closed_at_2_yea = Window.partitionBy(F.year("first_ticket_closed_at_2")).orderBy(F.col("prev_first_ticket_closed_at_2").asc_nulls_last(), "id")
w_prev_first_ticket_closed_at = Window.partitionBy(F.month("prev_first_ticket_closed_at")).orderBy("prev_first_ticket_closed_at")

valid_agents = (
    df_support_agents
    .filter(F.col("tier") == "T1")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.concat_ws("|", F.col("tier"), F.col("team")).alias("tier_team_label"),
        F.date_format(F.col("hired_on"), "MMM yyyy").alias("hired_on_label"),
    )
)

enriched = (
    valid_agents
    .select(
        "id",
        "agent_name",
        "tier",
        "tier_team_label",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.priority") == "P2"), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.min("closed_at").alias("first_ticket_closed_at_2"),
    )
)

profiled = (
    qualified.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_ticket_closed_at", "t.first_ticket_closed_at_2")
)

sequenced = (
    profiled
    .select(
        "id",
        "agent_name",
        "tier_team_label",
        "tier_or_unknown",
        "first_ticket_closed_at_2",
        F.lag("first_ticket_closed_at_2").over(w_tier_or_unknown_by_first_tic).alias("prev_first_ticket_closed_at_2"),
        F.round((F.unix_timestamp("first_ticket_closed_at_2") - F.unix_timestamp(F.lag("first_ticket_closed_at_2").over(w_tier_or_unknown_by_first_tic))) / 60, 1).alias("minutes_since_prev_first_ticket_closed_at_2"),
        F.rank().over(w_tier_or_unknown_by_first_tic_2).alias("first_ticket_closed_at_2_rank"),
        F.round((F.unix_timestamp("first_ticket_closed_at_2") - F.unix_timestamp(F.lag("first_ticket_closed_at_2").over(w_tier_tier_or_unknown_by_firs))) / 60, 1).alias("minutes_since_prev_first_ticket_closed_at_2_2"),
        F.lag("first_ticket_closed_at").over(w_tier_or_unknown_by_first_tic_2).alias("prev_first_ticket_closed_at"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.month(F.col("t.closed_at")).isin(1, 2)) & (F.col("t.requester_id") > 11), "left_semi")
)

with_trends = (
    filtered
    .withColumn("next2_first_ticket_closed_at_2", F.lead("first_ticket_closed_at_2", 2).over(w_first_ticket_closed_at_2_yea))
    .withColumn("prev_first_ticket_closed_at_rank", F.rank().over(w_prev_first_ticket_closed_at))
)

result = (
    with_trends
    .filter(F.length(F.trim(F.col("agent_name"))) >= 8)
    .select(
        "id",
        "agent_name",
        "tier_or_unknown",
        "first_ticket_closed_at_2",
        "minutes_since_prev_first_ticket_closed_at_2_2",
        "next2_first_ticket_closed_at_2",
        "prev_first_ticket_closed_at_rank",
    )
    .orderBy("agent_name", F.col("tier_or_unknown").desc())
)
