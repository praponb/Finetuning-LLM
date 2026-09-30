from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month_by_csat = Window.partitionBy(F.month("closed_at")).orderBy("csat", "id")
w_agent_id_agent_tier = Window.partitionBy("agent_id", "agent_tier")
w_closed_at_month_by_csat_full = Window.partitionBy(F.month("closed_at")).orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_month_by_csat_runn = Window.partitionBy(F.month("closed_at")).orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) & (F.col("channel") == "web"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

enriched_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.tags",
        F.col("su.tier").alias("agent_tier"),
    )
)

ranked = (
    enriched_agent
    .select(
        "*",
        F.dense_rank().over(w_closed_at_month_by_csat).alias("csat_dense_rank"),
        F.lead("csat").over(w_closed_at_month_by_csat).alias("next_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_agent_tier), 2).alias("csat_vs_agent_id_agent_tier_avg"),
        F.nth_value("closed_at", 2).over(w_closed_at_month_by_csat_full).alias("second_closed_at"),
        F.min("csat").over(w_closed_at_month_by_csat_runn).alias("min_csat_so_far"),
    )
)

joined = (
    ranked.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.lower(F.col("su.agent_name")).like("%to%")), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.agent_tier",
        "s.csat_dense_rank",
        "s.next_csat",
        "s.min_csat_so_far",
        "su.agent_name",
    )
)

result = (
    joined
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        "agent_tier",
        "csat_dense_rank",
        "next_csat",
        "min_csat_so_far",
        "agent_name",
        (F.col("next_csat") + F.col("csat")).alias("next_csat_plus_csat"),
    )
    .orderBy(F.col("agent_name").desc(), F.col("min_csat_so_far").asc_nulls_last())
)
