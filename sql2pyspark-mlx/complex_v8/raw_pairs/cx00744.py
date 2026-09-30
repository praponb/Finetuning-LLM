from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.to_date(F.col("closed_at")) < F.lit("2025-02-26").cast("date")) & (F.col("priority") == "P2"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

enriched = (
    recent
    .filter(F.col("requester_id") < 18)
    .select(
        "*",
        F.upper(F.trim(F.col("channel_priority_label"))).alias("channel_priority_label_clean"),
        F.round(F.col("requester_id_filled") / F.nullif(F.col("closed_at_quarter"), F.lit(0)), 3).alias("requester_id_filled_per_closed_at_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

exploded = (
    enriched
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "requester_id_filled",
        "closed_at_quarter",
        "channel_priority_label",
        "channel_priority_label_clean",
        "requester_id_filled_per_closed_at_quarter",
        "days_since_opened_at",
        F.posexplode_outer("tags").alias("tag_pos", "tag"),
    )
)

result = (
    exploded
    .select(
        "id",
        "agent_id",
        "opened_at",
        "requester_id_filled",
        "closed_at_quarter",
        "channel_priority_label",
        "channel_priority_label_clean",
        "requester_id_filled_per_closed_at_quarter",
        "days_since_opened_at",
        "tag",
        F.when(F.col("requester_id") >= 18, "large").when(F.col("requester_id") >= 9, "medium").otherwise("small").alias("requester_id_tier"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
