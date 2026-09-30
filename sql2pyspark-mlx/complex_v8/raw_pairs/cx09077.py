from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), F.col("id").desc())

recent = (
    df_support_agents
    .filter((F.col("agent_name").isNotNull()) & (F.year(F.col("hired_on")) == 2024))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

latest = (
    recent
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    latest
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        "team_slug",
        F.length(F.trim(F.col("agent_name"))).alias("agent_name_len"),
        F.last_day(F.col("hired_on")).alias("hired_on_month_end"),
        F.year(F.col("hired_on")).alias("hired_on_year"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
        F.count("*").alias("ticket_count_3"),
    )
)

profiled = (
    scored.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
        F.coalesce(F.col("t.ticket_count_3"), F.lit(0)).alias("ticket_count_3"),
    )
)

with_children_tickets = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.requester_id") > 3), "left")
    .select(
        "s.id",
        "s.team",
        "s.hired_on",
        "s.agent_name_len",
        "s.ticket_count",
        "s.ticket_count_2",
        "s.ticket_count_3",
        F.col("t.id").alias("ticket_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    with_children_tickets
    .cube("ticket_count_2", "agent_name_len", "ticket_channel")
    .agg(
        F.array_sort(F.collect_set("ticket_channel")).alias("ticket_channel_list"),
        F.min("hired_on").alias("first_hired_on"),
    )
    .select(
        "ticket_count_2",
        "agent_name_len",
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "ticket_channel_list",
        "first_hired_on",
    )
    .orderBy(
        F.col("ticket_count_2").asc_nulls_last(),
        F.col("agent_name_len").asc_nulls_last(),
        F.col("ticket_channel").asc_nulls_last(),
    )
)
