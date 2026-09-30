from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id", "channel").orderBy(F.col("minutes_closed_at_to_opened_at").desc(), F.col("id").desc())

base = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-02-12 10:00:00").cast("timestamp")) & (F.col("requester_id") >= 7))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
    )
)

unique_rows = (
    base
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    unique_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") == "email") & (F.col("t.csat").between(2, 5)), "left_semi")
)

agent_id_stats = (
    filtered
    .groupBy("agent_id")
    .agg(F.percentile_approx("minutes_closed_at_to_opened_at", 0.5).alias("median_minutes_closed_at_to_opened_at"))
)

compared = (
    filtered.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.minutes_closed_at_to_opened_at") >= F.col("g.median_minutes_closed_at_to_opened_at"))
    .select(
        "s.*",
        "g.median_minutes_closed_at_to_opened_at",
        F.round(F.col("s.minutes_closed_at_to_opened_at") - F.col("g.median_minutes_closed_at_to_opened_at"), 2).alias("minutes_closed_at_to_opened_at_minus_median"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority") == "P2", 1)).alias("ticket_priority_count"),
        F.count("*").alias("ticket_count"),
    )
)

profiled = (
    compared.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

combined_details = (
    profiled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.minutes_closed_at_to_opened_at",
        "s.median_minutes_closed_at_to_opened_at",
        "s.minutes_closed_at_to_opened_at_minus_median",
        "s.ticket_priority_count",
        "s.ticket_count",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_parent_agent = (
    combined_details.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.team").alias("agent_team"))
)

result = (
    with_parent_agent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "minutes_closed_at_to_opened_at",
        "median_minutes_closed_at_to_opened_at",
        "minutes_closed_at_to_opened_at_minus_median",
        "ticket_priority_count",
        "ticket_count",
        "agent_hired_on",
        "agent_team",
    )
    .orderBy(F.col("minutes_closed_at_to_opened_at_minus_median").desc())
)
