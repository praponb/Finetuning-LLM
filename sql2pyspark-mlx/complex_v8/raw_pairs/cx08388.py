from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tier").orderBy(F.col("hired_on").desc(), F.col("id").desc(), F.col("ticket_id").desc())

scoped_agents = (
    df_support_agents
    .filter((F.col("team") == "Billing") & (F.upper(F.trim(F.col("agent_name"))).like("I%")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.substring(F.trim(F.col("team")), 1, 4).alias("team_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("tier")), " ", "_")).alias("tier_slug"),
    )
)

detail_rows_tickets = (
    scoped_agents.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.team",
        "s.tier",
        "s.hired_on",
        F.col("t.id").alias("ticket_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_details_ticket = (
    detail_rows_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") != "P4"), "left")
    .select("s.*", F.col("t.requester_id").alias("ticket_requester_id"))
)

scored = (
    with_details_ticket
    .filter(F.col("tier") == "T3")
    .select(
        "*",
        F.date_format(F.col("hired_on"), "MMM yyyy").alias("hired_on_label"),
        (F.col("ticket_closed_at") + F.expr("INTERVAL 2 HOURS")).alias("ticket_closed_at_plus_2h"),
        F.concat(F.col("tier"), F.lit("#"), F.col("id").cast("string")).alias("tier_ref"),
    )
)

unique_rows = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined_ticket = (
    unique_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.team",
        "s.ticket_id",
        "s.ticket_closed_at",
        "s.ticket_requester_id",
        "s.hired_on_label",
        "s.ticket_closed_at_plus_2h",
        "s.tier_ref",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

grouped = (
    joined_ticket
    .groupBy("ticket_id", "ticket_agent_id")
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("agent_name"))).like("K%"), F.col("ticket_requester_id")).otherwise(0)).alias("ticket_requester_id_upper_trim_agent_name_like_k"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.round(F.avg("ticket_requester_id"), 2).alias("avg_ticket_requester_id"),
        F.avg("ticket_requester_id").alias("avg_ticket_requester_id_check"),
    )
    .filter(F.col("avg_ticket_requester_id_check") > 7)
    .drop("avg_ticket_requester_id_check")
)

result = (
    grouped
    .groupingSets(
        [["ticket_id", "ticket_agent_id", "n_rows"], ["ticket_id"], []],
        "ticket_id", "ticket_agent_id", "n_rows",
    )
    .agg(
        F.round(F.stddev("avg_ticket_requester_id"), 2).alias("stddev_avg_ticket_requester_id"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_agent_id").asc_nulls_last(),
        F.col("n_rows").asc_nulls_last(),
    )
)
