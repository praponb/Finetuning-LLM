from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id = Window.partitionBy("event_type", "ticket_id")
w_ticket_id_event_at_to_date_b = Window.partitionBy("ticket_id", F.to_date("event_at")).orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_at_to_date_b_2 = Window.partitionBy("ticket_id", F.to_date("event_at")).orderBy("days_since_event_at", "id").rowsBetween(-2, Window.currentRow)
w_dedup = Window.partitionBy("ticket_id", "actor").orderBy("rows_in_event_type_ticket_id", "id")

scoped = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

ranked = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "days_since_event_at",
        "actor_len",
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_event_type_ticket_id), F.lit(0)), 4).alias("days_since_event_at_share_of_event_type_ticket_i"),
        F.sum("actor_len").over(w_ticket_id_event_at_to_date_b).alias("running_actor_len"),
        F.round(F.col("actor_len") - F.avg("actor_len").over(w_event_type_ticket_id), 2).alias("actor_len_vs_event_type_ticket_id_avg"),
        F.count(F.lit(1)).over(w_event_type_ticket_id).alias("rows_in_event_type_ticket_id"),
        F.max("actor_len").over(w_ticket_id_event_at_to_date_b_2).alias("actor_len_max_3"),
    )
)

with_parent = (
    ranked.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

unique_rows = (
    with_parent
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "actor",
        "days_since_event_at",
        "days_since_event_at_share_of_event_type_ticket_i",
        "rows_in_event_type_ticket_id",
        "actor_len_max_3",
        "ticket_opened_at",
        "ticket_priority",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.when(F.col("actor_len_max_3") >= 36, "HIGH").when(F.col("actor_len_max_3") >= 14, "MEDIUM").otherwise("LOW").alias("actor_len_max_3_tier"),
    )
    .orderBy("actor", F.col("ticket_opened_at").asc_nulls_last(), "id")
    .limit(20)
)
