from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_month = Window.partitionBy(F.month("hired_on"))
w_ticket_count_by_ticket_count = Window.partitionBy("ticket_count").orderBy(F.col("ticket_count").asc_nulls_last(), "id")
w_ticket_count_by_ticket_count_2 = Window.partitionBy("ticket_count").orderBy(F.col("ticket_count").asc_nulls_last(), "id").rowsBetween(-6, -1)

base = (
    df_support_agents
    .filter((F.col("tier").isin("T3", "T2")) & (F.col("team") == "Onboarding"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.concat_ws(": ", F.col("tier"), F.col("team")).alias("tier_team_label"),
        F.lower(F.regexp_replace(F.trim(F.col("tier")), " ", "_")).alias("tier_slug"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count(F.when(~F.col("channel").isin("email", "phone"), 1)).alias("ticket_channel_count"),
        F.count("*").alias("ticket_count_2"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.round(F.avg("ticket_count_2").over(w_hired_on_month), 2).alias("hired_on_month_avg_ticket_count_2"),
        F.row_number().over(w_ticket_count_by_ticket_count).alias("rn"),
        F.max("ticket_count").over(w_ticket_count_by_ticket_count_2).alias("ticket_count_max_6_prior"),
        F.min("ticket_channel_count").over(w_hired_on_month).alias("hired_on_month_min_ticket_channel_count"),
        F.count(F.lit(1)).over(w_hired_on_month).alias("rows_in_hired_on_month"),
    )
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("requester_id").alias("max_ticket_requester_id"),
        F.round(F.avg("csat"), 2).alias("avg_ticket_csat"),
    )
)

summarized_rows_2 = (
    with_trends.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.max_ticket_requester_id", "t.avg_ticket_csat")
)

result = (
    summarized_rows_2
    .select(
        "id",
        F.col("rows_in_hired_on_month").alias("m_rows_in_hired_on_month"),
        F.col("rn").alias("rn_v"),
        F.col("ticket_count_2").alias("ticket_count_2_v"),
        F.col("hired_on_month_min_ticket_channel_count").alias("m_hired_on_month_min_ticket_channel_count"),
    )
    .unpivot(
        ids=["id"],
        values=["m_rows_in_hired_on_month", "rn_v", "ticket_count_2_v", "m_hired_on_month_min_ticket_channel_count"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
