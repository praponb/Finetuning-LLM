from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority", "agent_id").orderBy("requester_id_per_csat_minus_max_minus_median", "id", "event_id")

valid_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (~F.col("channel").isin("chat", "email")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

priority_stats = (
    valid_tickets
    .groupBy("priority")
    .agg(F.max("requester_id_per_csat").alias("max_requester_id_per_csat"))
)

outliers = (
    valid_tickets.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.requester_id_per_csat") == F.col("g.max_requester_id_per_csat"))
    .select(
        "s.*",
        "g.max_requester_id_per_csat",
        F.round(F.col("s.requester_id_per_csat") - F.col("g.max_requester_id_per_csat"), 2).alias("requester_id_per_csat_minus_max"),
    )
)

agent_id_stats = (
    outliers
    .groupBy("agent_id")
    .agg(F.percentile_approx("requester_id_per_csat_minus_max", 0.5).alias("median_requester_id_per_csat_minus_max"))
)

above_average = (
    outliers.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id_per_csat_minus_max") >= F.col("g.median_requester_id_per_csat_minus_max"))
    .select(
        "s.*",
        "g.median_requester_id_per_csat_minus_max",
        F.round(F.col("s.requester_id_per_csat_minus_max") - F.col("g.median_requester_id_per_csat_minus_max"), 2).alias("requester_id_per_csat_minus_max_minus_median"),
    )
)

line_level_events = (
    above_average.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.max_requester_id_per_csat",
        "s.requester_id_per_csat_minus_max",
        "s.median_requester_id_per_csat_minus_max",
        "s.requester_id_per_csat_minus_max_minus_median",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

latest = (
    line_level_events
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    latest
    .filter(F.col("requester_id_per_csat_minus_max").between(-6.0, 2.0))
    .select(
        "*",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled_2"),
    )
)

result = (
    labeled
    .groupBy("csat_filled")
    .pivot("priority", ["P3", "P1"])
    .agg(F.sum("requester_id_per_csat_minus_max_minus_median").alias("amount"), F.max("requester_id_per_csat_minus_max_minus_median").alias("max_v"))
)
