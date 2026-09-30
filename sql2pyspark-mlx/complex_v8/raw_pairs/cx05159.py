from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy("requester_id", "id")

base_tickets = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

unique_rows = (
    base_tickets
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

qualified = (
    unique_rows.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("G%")) & (F.col("su.tier") != "T1"), "left_semi")
)

enriched = (
    qualified
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "tags",
        "tags_count",
        F.coalesce(F.col("tags_count"), F.lit(0)).alias("tags_count_filled"),
        F.when(F.col("priority").isin("P4", "P3"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
    )
)

scored = (
    enriched
    .filter(F.col("priority_group") == "group_a")
    .select(
        "id",
        "priority",
        "tags_count",
        "tags_count_filled",
        "priority_group",
        "closed_at_label",
        F.when(F.col("tags_count_filled") >= 2, "large").when(F.col("tags_count_filled") >= 1, "medium").otherwise("small").alias("tags_count_filled_tier"),
    )
)

screened = (
    scored.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("tags_count") < 2)
)

result = (
    screened
    .select(
        "id",
        "priority",
        "tags_count",
        "tags_count_filled",
        "priority_group",
        "closed_at_label",
        F.when(F.col("tags_count_filled_tier").isin("medium", "large"), "group_a").when(F.col("tags_count_filled_tier") == "small", "group_b").otherwise("other").alias("tags_count_filled_tier_group"),
    )
    .orderBy(F.col("tags_count_filled").asc())
)
