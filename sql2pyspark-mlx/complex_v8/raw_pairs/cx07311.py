from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("tier_or_unknown", "tier").orderBy(F.col("hired_on_plus_365d").desc(), F.col("id").desc())

filtered = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%no%")) & (F.col("tier") == "T2"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.concat_ws("-", F.col("tier"), F.col("agent_name")).alias("tier_agent_name_label"),
        F.when(F.col("tier").isin("T1", "T2"), "group_a").when(F.col("tier") == "T3", "group_b").otherwise("other").alias("tier_group"),
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

labeled = (
    filtered
    .select(
        "id",
        "agent_name",
        "tier",
        "tier_or_unknown",
        F.date_add(F.col("hired_on"), 365).alias("hired_on_plus_365d"),
        F.year(F.col("hired_on")).alias("hired_on_year"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

unique_rows = (
    labeled
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

tagged = (
    unique_rows
    .filter(F.col("hired_on_plus_365d").isNotNull())
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on_plus_365d",
        "hired_on_year",
        "team_slug",
        F.coalesce(F.col("tier_or_unknown"), F.lit("UNKNOWN")).alias("tier_or_unknown_or_unknown"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.min("opened_at").alias("first_ticket_opened_at_2"),
    )
)

profiled = (
    tagged.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_ticket_opened_at",
        "t.first_ticket_opened_at",
        "t.first_ticket_opened_at_2",
    )
)

result = (
    profiled
    .filter(F.col("agent_name").isNotNull())
    .select(
        "id",
        "agent_name",
        "team_slug",
        "last_ticket_opened_at",
        F.when(F.col("tier").isin("T1", "T2"), "group_a").when(F.col("tier") == "T3", "group_b").otherwise("other").alias("tier_group"),
    )
    .orderBy(F.col("id").desc(), F.col("last_ticket_opened_at").desc())
    .limit(10)
)
