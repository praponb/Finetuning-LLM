from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_dayofweek_by_hired = Window.partitionBy("hired_on_dayofweek").orderBy("hired_on_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team_by_hired_on_dayofweek_t = Window.partitionBy("team").orderBy("hired_on_dayofweek")

filtered_agents = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%is%")) & (F.col("hired_on") < F.lit("2025-02-26").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.concat(F.col("tier"), F.lit("#"), F.col("id").cast("string")).alias("tier_ref"),
        F.date_format(F.col("hired_on"), "MMM yyyy").alias("hired_on_label"),
        F.date_trunc("week", F.col("hired_on")).alias("hired_on_week"),
    )
)

scored = (
    filtered_agents
    .select(
        "id",
        "agent_name",
        "team",
        "tier_ref",
        "hired_on_label",
        "hired_on_week",
        F.dayofweek(F.col("hired_on")).alias("hired_on_dayofweek"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "agent_name",
        "tier_ref",
        "hired_on_week",
        "hired_on_dayofweek",
        F.first("hired_on_week", ignorenulls=True).over(w_hired_on_dayofweek_by_hired).alias("first_known_hired_on_week"),
        F.round(F.percent_rank().over(w_team_by_hired_on_dayofweek_t), 3).alias("hired_on_dayofweek_percent_rank"),
    )
)

with_children_tickets = (
    metrics.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.tier_ref",
        "s.hired_on_week",
        "s.first_known_hired_on_week",
        "s.hired_on_dayofweek_percent_rank",
        F.col("t.id").alias("ticket_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    with_children_tickets
    .cube("ticket_priority", "ticket_id", "ticket_csat")
    .agg(
        F.round(F.sum("hired_on_dayofweek_percent_rank") / F.nullif(F.sum("ticket_csat"), F.lit(0)), 3).alias("hired_on_dayofweek_percent_rank_to_ticket_csat_r"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("ticket_priority").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_csat").asc_nulls_last(),
    )
)
