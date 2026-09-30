from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_year_by_reque = Window.partitionBy(F.year("agent_hired_on")).orderBy("requester_id", "id")
w_closed_at_month_agent_id = Window.partitionBy(F.month("closed_at"), "agent_id")
w_agent_hired_on_year_by_reque_2 = Window.partitionBy(F.year("agent_hired_on")).orderBy("requester_id")
w_agent_hired_on_year_by_reque_3 = Window.partitionBy(F.year("agent_hired_on")).orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_channel_by_agent = Window.partitionBy("agent_team", "channel").orderBy(F.col("agent_hired_on").desc(), "id")
w_agent_team_channel_by_agent_2 = Window.partitionBy("agent_team", "channel").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(-1, Window.currentRow)
w_agent_team_channel_by_agent_3 = Window.partitionBy("agent_team", "channel").orderBy(F.col("agent_hired_on").desc())
w_cnt_agent_hired_on_seq_by_di = Window.partitionBy("cnt", "agent_hired_on_seq").orderBy(F.col("distinct_agent_teams").desc(), "agent_hired_on_seq", "opened_at_month").rowsBetween(-3, Window.currentRow)
w_distinct_agent_teams = Window.partitionBy("distinct_agent_teams")
w_cnt_agent_hired_on_seq_by_di_2 = Window.partitionBy("cnt", "agent_hired_on_seq").orderBy(F.col("distinct_agent_teams").desc(), "agent_hired_on_seq", "opened_at_month")

cleaned_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

enriched_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") == "VIP")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

ranked = (
    enriched_agent
    .select(
        "*",
        F.ntile(3).over(w_agent_hired_on_year_by_reque).alias("requester_id_tercile"),
        F.count(F.lit(1)).over(w_closed_at_month_agent_id).alias("rows_in_closed_at_month_agent_id"),
        F.dense_rank().over(w_agent_hired_on_year_by_reque_2).alias("requester_id_dense_rank"),
        F.first("requester_id", ignorenulls=True).over(w_agent_hired_on_year_by_reque_3).alias("first_known_requester_id"),
        F.lag("opened_at").over(w_agent_hired_on_year_by_reque).alias("prev_opened_at"),
        F.row_number().over(w_agent_team_channel_by_agent).alias("agent_hired_on_seq"),
        F.round(F.avg("requester_id").over(w_agent_team_channel_by_agent_2), 2).alias("requester_id_avg_2"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_month_agent_id), F.lit(0)), 4).alias("requester_id_share_of_closed_at_month_agent_id"),
        F.size(F.collect_set("channel").over(w_closed_at_month_agent_id)).alias("distinct_channel_in_closed_at_month_agent_id"),
        F.round(F.cume_dist().over(w_agent_team_channel_by_agent_3), 3).alias("agent_hired_on_cume_dist"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_month_agent_id), 2).alias("requester_id_vs_closed_at_month_agent_id_avg"),
    )
)

top_rows = ranked.filter(F.col("requester_id_dense_rank") <= 3)

summary = (
    top_rows
    .filter(F.col("distinct_channel_in_closed_at_month_agent_id") > 2)
    .groupBy(
        "agent_hired_on_seq",
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
        F.count("*").alias("cnt"),
    )
)

with_windows = (
    summary
    .select(
        "*",
        F.round(F.avg("agent_hired_on_seq").over(w_cnt_agent_hired_on_seq_by_di), 2).alias("agent_hired_on_seq_avg_4"),
        F.lead("agent_hired_on_seq", 2).over(w_cnt_agent_hired_on_seq_by_di_2).alias("next2_agent_hired_on_seq"),
    )
)

result = (
    with_windows
    .select(
        "agent_hired_on_seq",
        "distinct_agent_teams",
        "cnt",
        "agent_hired_on_seq_avg_4",
        "next2_agent_hired_on_seq",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at_month")).alias("days_since_opened_at_month"),
        F.date_format(F.col("opened_at_month"), "MMM yyyy").alias("opened_at_month_label"),
    )
)
