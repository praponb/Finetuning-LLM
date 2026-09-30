from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_running = Window.orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat_full = Window.orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_csat_moving_2 = Window.orderBy("csat", "id").rowsBetween(-2, Window.currentRow)
w_csat_agent_hired_on_month = Window.partitionBy("csat", F.month("agent_hired_on"))

recent = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(6, 11))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

with_details_agent = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    with_details_agent
    .select(
        "*",
        F.last("agent_hired_on", ignorenulls=True).over(w_all_by_csat_running).alias("last_known_agent_hired_on"),
        F.last("requester_id").over(w_all_by_csat_full).alias("last_requester_id"),
        F.sum("csat").over(w_all_by_csat_moving_2).alias("csat_sum_3"),
        F.size(F.collect_set("agent_id").over(w_csat_agent_hired_on_month)).alias("distinct_agent_id_in_csat_agent_hired_on_month"),
    )
)

tagged = (
    windowed
    .select(
        "*",
        F.when(F.col("csat_sum_3") >= 10, "A").when(F.col("csat_sum_3") >= 4, "B").otherwise("C").alias("csat_sum_3_tier"),
        F.concat(F.col("priority"), F.lit("#"), F.col("last_requester_id").cast("string")).alias("priority_ref"),
        F.add_months(F.col("agent_hired_on"), 12).alias("agent_hired_on_plus_12m"),
    )
)

result = (
    tagged
    .select(
        "id",
        "agent_id",
        "priority",
        "agent_hired_on",
        "agent_tier",
        "agent_team",
        "last_known_agent_hired_on",
        "last_requester_id",
        "csat_sum_3",
        "distinct_agent_id_in_csat_agent_hired_on_month",
        "csat_sum_3_tier",
        "priority_ref",
        "agent_hired_on_plus_12m",
        F.size(F.col("tags")).alias("tags_count"),
        F.when(F.col("csat_sum_3") >= 9, "HIGH").when(F.col("csat_sum_3") >= 4, "MEDIUM").otherwise("LOW").alias("csat_sum_3_tier_2"),
    )
    .orderBy(F.col("last_requester_id").desc())
)
