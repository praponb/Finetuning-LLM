from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(1, 2)) & (F.col("channel") == "email"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

profiled = (
    base.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at")
)

agent_id_stats = (
    profiled
    .groupBy("agent_id")
    .agg(F.percentile_approx("csat_filled", 0.5).alias("median_csat_filled"))
)

above_average = (
    profiled.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat_filled") >= F.col("g.median_csat_filled"))
    .select(
        "s.*",
        "g.median_csat_filled",
        F.round(F.col("s.csat_filled") - F.col("g.median_csat_filled"), 2).alias("csat_filled_minus_median"),
    )
)

result = (
    above_average
    .filter(F.col("requester_id") < 22)
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "closed_at_label",
        "hours_closed_at_to_opened_at",
        "first_event_at",
        "median_csat_filled",
        "csat_filled_minus_median",
        F.lower(F.trim(F.col("closed_at_label"))).alias("closed_at_label_clean"),
        F.substring(F.trim(F.col("closed_at_label")), 1, 2).alias("closed_at_label_prefix"),
    )
    .orderBy(F.col("csat").desc_nulls_first(), "id")
)
