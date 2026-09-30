from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id_by_request = Window.partitionBy("priority", "agent_id").orderBy("requester_id_minus_max", "id")
w_all_by_max_requester_id = Window.orderBy(F.col("max_requester_id").desc(), "id")
w_agent_id_by_requester_id_run = Window.partitionBy("agent_id").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_requester_id_mov = Window.partitionBy("agent_id").orderBy("requester_id", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-21 07:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
    )
)

agent_id_stats = (
    cleaned
    .groupBy("agent_id")
    .agg(F.max("requester_id").alias("max_requester_id"))
)

outliers = (
    cleaned.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id") == F.col("g.max_requester_id"))
    .select(
        "s.*",
        "g.max_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.max_requester_id"), 2).alias("requester_id_minus_max"),
    )
)

ranked = (
    outliers
    .select(
        "*",
        F.ntile(5).over(w_priority_agent_id_by_request).alias("requester_id_minus_max_quintile"),
        F.ntile(10).over(w_all_by_max_requester_id).alias("max_requester_id_decile"),
        F.last("max_requester_id", ignorenulls=True).over(w_agent_id_by_requester_id_run).alias("last_known_max_requester_id"),
        F.lead("requester_id_minus_max", 1, 0).over(w_priority_agent_id_by_request).alias("next_requester_id_minus_max"),
        F.round(F.avg("requester_id").over(w_agent_id_by_requester_id_mov), 2).alias("requester_id_avg_7"),
    )
)

agent_id_stats_2 = (
    ranked
    .groupBy("agent_id")
    .agg(F.percentile_approx("max_requester_id_decile", 0.5).alias("median_max_requester_id_decile"))
)

above_average = (
    ranked.alias("s")
    .join(agent_id_stats_2.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.max_requester_id_decile") >= F.col("g.median_max_requester_id_decile"))
    .select(
        "s.*",
        "g.median_max_requester_id_decile",
        F.round(F.col("s.max_requester_id_decile") - F.col("g.median_max_requester_id_decile"), 2).alias("max_requester_id_decile_minus_median"),
    )
)

result = (
    above_average
    .select(
        "id",
        "requester_id",
        "agent_id",
        "max_requester_id",
        "requester_id_minus_max",
        "requester_id_minus_max_quintile",
        "max_requester_id_decile",
        "next_requester_id_minus_max",
        "requester_id_avg_7",
        "median_max_requester_id_decile",
        "max_requester_id_decile_minus_median",
        F.abs(F.col("requester_id") - 7).alias("requester_id_distance"),
    )
    .orderBy(
        F.col("max_requester_id_decile_minus_median").asc(),
        F.col("max_requester_id").desc(),
        "id",
    )
    .limit(5)
)
