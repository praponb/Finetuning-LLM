from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_priority_len_agent_id_by_pri = Window.partitionBy("priority_len", "agent_id").orderBy(F.col("priority_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .filter((F.col("priority") == "P3") & (F.col("channel") != "web"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.csat",
        "s.priority_len",
        "s.opened_at_label",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.min("priority_len").over(w_agent_id).alias("agent_id_min_priority_len"),
        F.size(F.collect_set("agent_tier").over(w_agent_id)).alias("distinct_agent_tier_in_agent_id"),
        F.sum("csat").over(w_priority_len_agent_id_by_pri).alias("running_csat"),
    )
)

agent_tier_stats = (
    with_windows
    .groupBy("agent_tier")
    .agg(F.max("priority_len").alias("max_priority_len"))
)

outliers = (
    with_windows.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.priority_len") == F.col("g.max_priority_len"))
    .select(
        "s.*",
        "g.max_priority_len",
        F.round(F.col("s.priority_len") - F.col("g.max_priority_len"), 2).alias("priority_len_minus_max"),
    )
)

result = (
    outliers
    .cube("agent_tier", "priority_len")
    .agg(
        F.datediff(F.max("agent_hired_on"), F.min("agent_hired_on")).alias("agent_hired_on_span_days"),
        F.round(F.sum("max_priority_len") / F.nullif(F.sum("agent_id_min_priority_len"), F.lit(0)), 3).alias("max_priority_len_to_agent_id_min_priority_len_ra"),
    )
)
