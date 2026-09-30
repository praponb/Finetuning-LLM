from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter(F.col("priority").isNotNull())
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        F.lower(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

qualified = (
    base_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.opened_at") >= F.lit("2025-01-31 06:00:00").cast("timestamp")) & (F.col("t.priority") == "P3"), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
        "priority_clean",
        F.lower(F.regexp_replace(F.trim(F.col("priority_clean")), " ", "_")).alias("priority_clean_slug"),
        F.when(F.col("csat") >= 4, "large").when(F.col("csat") >= 3, "medium").otherwise("small").alias("csat_tier"),
    )
    .orderBy("agent_id", F.col("closed_at").desc_nulls_first())
)
