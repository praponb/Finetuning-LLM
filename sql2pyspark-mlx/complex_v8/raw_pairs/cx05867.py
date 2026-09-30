from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("priority_or_unknown").orderBy("requester_id_filled_minus_csat", "id")

cleaned_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-02-08 22:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        (F.col("closed_at") + F.expr("INTERVAL 15 MINUTES")).alias("closed_at_plus_15m"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
    )
)

priority_or_unknown_stats = (
    cleaned_tickets
    .groupBy("priority_or_unknown")
    .agg(F.avg("csat").alias("avg_csat"))
)

outliers = (
    cleaned_tickets.alias("s")
    .join(priority_or_unknown_stats.alias("g"), F.col("g.priority_or_unknown") == F.col("s.priority_or_unknown"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

derived = (
    outliers
    .filter(F.col("agent_id").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        F.when(F.col("requester_id") >= 21, "HIGH").when(F.col("requester_id") >= 8, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
    )
)

annotated = (
    derived
    .filter(F.array_contains(F.col("tags"), "bug"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        "priority_or_unknown",
        "closed_at_label",
        "avg_csat",
        "csat_minus_avg",
        "requester_id_filled",
        F.date_trunc("day", F.col("closed_at")).alias("closed_at_day"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled_2"),
        (F.col("requester_id_filled") - F.col("csat")).alias("requester_id_filled_minus_csat"),
    )
)

qualified = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority").isNotNull()) & (F.col("t.csat") >= 2), "left_semi")
)

without_matches = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (~F.col("t.channel").isin("phone", "web")), "left_anti")
)

deduped = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        "priority_or_unknown",
        "closed_at_label",
        "avg_csat",
        "requester_id_filled",
        "closed_at_day",
        "requester_id_filled_2",
        F.round(F.col("requester_id_filled_2") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_filled_2_per_csat"),
        F.when(F.col("avg_csat") >= 3.4, "HIGH").when(F.col("avg_csat") >= 2.4, "MEDIUM").otherwise("LOW").alias("avg_csat_tier"),
    )
    .orderBy(F.col("closed_at_day").desc())
)
