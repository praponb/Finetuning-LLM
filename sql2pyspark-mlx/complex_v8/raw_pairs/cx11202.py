from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_len_by_avg_csat_run = Window.partitionBy("priority_len").orderBy("avg_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_tier_agent_id = Window.partitionBy("csat_tier", "agent_id")
w_all_by_priority_len_ties = Window.orderBy("priority_len")
w_all_by_priority_len = Window.orderBy("priority_len", "id")

base = (
    df_tickets
    .filter((F.col("requester_id") <= 18) & (F.col("csat") > 1))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        F.when(F.col("csat") >= 4, "HIGH").when(F.col("csat") >= 3, "MEDIUM").otherwise("LOW").alias("csat_tier"),
    )
)

csat_tier_stats = (
    base
    .groupBy("csat_tier")
    .agg(F.avg("csat").alias("avg_csat"))
)

above_average = (
    base.alias("s")
    .join(csat_tier_stats.alias("g"), F.col("g.csat_tier") == F.col("s.csat_tier"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

scored = (
    above_average
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("csat_tier")), " ", "_")).alias("csat_tier_slug"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") > 3), "left_anti")
    .filter(F.col("csat") >= 3)
)

sequenced = (
    qualified
    .select(
        "*",
        F.last("csat", ignorenulls=True).over(w_priority_len_by_avg_csat_run).alias("last_known_csat"),
        F.max("priority_len").over(w_csat_tier_agent_id).alias("csat_tier_agent_id_max_priority_len"),
        F.max("avg_csat").over(w_csat_tier_agent_id).alias("csat_tier_agent_id_max_avg_csat"),
        F.round(F.percent_rank().over(w_all_by_priority_len_ties), 3).alias("priority_len_percent_rank"),
        F.round(F.avg("priority_len").over(w_csat_tier_agent_id), 2).alias("csat_tier_agent_id_avg_priority_len"),
        F.round(F.percent_rank().over(w_all_by_priority_len), 3).alias("priority_len_percent_rank_2"),
    )
)

qualified_2 = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.to_date(F.col("t.opened_at")) < F.lit("2025-02-12").cast("date")) & (F.size(F.col("t.tags")) >= 1), "left_semi")
)

with_parent_agent = (
    qualified_2.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.csat_tier",
        "s.last_known_csat",
        "s.csat_tier_agent_id_max_avg_csat",
        "s.priority_len_percent_rank",
        "s.priority_len_percent_rank_2",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_parent_agent
    .groupBy("closed_at", "agent_tier")
    .pivot("csat_tier", ["HIGH", "LOW", "MEDIUM"])
    .agg(F.sum("priority_len_percent_rank"))
)
