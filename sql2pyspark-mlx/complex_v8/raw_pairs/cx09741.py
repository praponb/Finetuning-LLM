from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((~F.col("priority").isin("P1", "P4")) | (F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
)

exploded = (
    recent
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "priority_or_unknown",
        "requester_id_filled",
        "priority_prefix",
        F.explode("tags").alias("tag"),
    )
)

tagged = (
    exploded
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("tag")), " ", "_")).alias("tag_slug"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

tagged_2 = (
    tagged
    .select(
        "*",
        F.when(F.col("priority_len") >= 10, "A").when(F.col("priority_len") >= 6, "B").otherwise("C").alias("priority_len_tier"),
        F.concat_ws("|", F.col("tag"), F.col("priority_or_unknown")).alias("tag_priority_or_unknown_label"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

result = (
    tagged_2
    .rollup("priority", "agent_id", "priority_or_unknown")
    .agg(
        F.round(F.sum("requester_id_filled") / F.nullif(F.sum("hours_opened_at_to_closed_at"), F.lit(0)), 3).alias("requester_id_filled_to_hours_opened_at_to_closed"),
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("priority_or_unknown").asc_nulls_last(),
    )
)
