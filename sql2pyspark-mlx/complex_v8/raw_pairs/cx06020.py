from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at")
w_actor_event_at_month_by_days = Window.partitionBy("actor", F.month("event_at")).orderBy("days_since_event_at", "id")
w_event_at_month_actor = Window.partitionBy(F.month("event_at"), "actor")

base = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(6, 8))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.rank().over(w_ticket_id_actor_by_event_at).alias("event_at_rank"),
        F.lead("days_since_event_at").over(w_actor_event_at_month_by_days).alias("next_days_since_event_at"),
        F.row_number().over(w_actor_event_at_month_by_days).alias("rn"),
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_event_at_month_actor), 2).alias("days_since_event_at_vs_event_at_month_actor_avg"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.substring(F.trim(F.col("event_type_actor_label")), 1, 4).alias("event_type_actor_label_prefix"),
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
    )
)

first_rows = (
    tagged
    .filter((F.col("rn") == 1) & (F.col("event_at_date").between(F.lit("2024-02-01").cast("date"), F.lit("2025-10-01").cast("date"))))
    .drop("rn")
)

result = (
    first_rows
    .groupingSets(
        [["ticket_id", "event_type", "event_at_rank"], ["ticket_id"], ["event_type"], []],
        "ticket_id", "event_type", "event_at_rank",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("event_at_rank"), 2).alias("stddev_event_at_rank"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "event_at_rank",
        "row_count",
        "stddev_event_at_rank",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("event_at_rank").asc_nulls_last(),
    )
)
