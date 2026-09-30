from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_by_requester = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy("requester_id_filled", "id")
w_csat_tier_csat = Window.partitionBy("csat_tier", "csat")
w_csat_tier_agent_id_by_reques = Window.partitionBy("csat_tier", "agent_id").orderBy("requester_id_filled", "id")

prepared_tickets = (
    df_tickets
    .filter((F.col("requester_id") <= 14) & (F.col("csat") > 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.when(F.col("csat") >= 5, "HIGH").when(F.col("csat") >= 3, "MEDIUM").otherwise("LOW").alias("csat_tier"),
    )
)

csat_tier_stats = (
    prepared_tickets
    .groupBy("csat_tier")
    .agg(F.max("requester_id_filled").alias("max_requester_id_filled"))
)

above_average = (
    prepared_tickets.alias("s")
    .join(csat_tier_stats.alias("g"), F.col("g.csat_tier") == F.col("s.csat_tier"), "inner")
    .filter(F.col("s.requester_id_filled") == F.col("g.max_requester_id_filled"))
    .select(
        "s.*",
        "g.max_requester_id_filled",
        F.round(F.col("s.requester_id_filled") - F.col("g.max_requester_id_filled"), 2).alias("requester_id_filled_minus_max"),
    )
)

ranked = (
    above_average
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "csat_tier",
        "max_requester_id_filled",
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_opened_at_month_by_requester))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.round(F.avg("greatest_csat_requester_id").over(w_csat_tier_csat), 2).alias("csat_tier_csat_avg_greatest_csat_requester_id"),
        F.ntile(3).over(w_csat_tier_agent_id_by_reques).alias("requester_id_filled_tercile"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("csat").alias("max_ticket_csat"),
        F.count("*").alias("ticket_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_counts = (
    ranked.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.max_ticket_csat",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_closed_at",
    )
)

result = (
    with_counts
    .filter(F.col("csat_tier").isin("HIGH", "LOW"))
    .select(
        "id",
        "agent_id",
        "csat_tier",
        "max_requester_id_filled",
        "minutes_since_prev_opened_at",
        "requester_id_filled_tercile",
        "ticket_count",
        "last_ticket_closed_at",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
    .orderBy("last_ticket_closed_at", F.col("max_requester_id_filled").desc())
)
