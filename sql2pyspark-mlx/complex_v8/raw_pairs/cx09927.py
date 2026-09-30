from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_latest = Window.partitionBy("actor").orderBy(F.col("next3_event_at").desc(), F.col("id").desc())

prepared = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") < 5), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

ranked = (
    joined_ticket
    .select(
        "*",
        F.lead("event_at", 3).over(w_ticket_id_by_event_at).alias("next3_event_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("seq_no"),
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at"),
        F.ntile(10).over(w_ticket_id_by_event_at).alias("event_at_decile"),
        F.dense_rank().over(w_ticket_id_by_event_at_ties).alias("event_at_dense_rank"),
    )
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    deduped
    .select(
        "*",
        F.date_format(F.col("prev_event_at"), "yyyy-MM-dd HH:00").alias("prev_event_at_label"),
        F.coalesce(F.col("event_at_decile"), F.lit(0)).alias("event_at_decile_filled"),
        F.round(F.col("event_at_decile") / F.nullif(F.col("event_at_dense_rank"), F.lit(0)), 3).alias("event_at_decile_per_event_at_dense_rank"),
    )
)

tagged = (
    enriched
    .filter(F.hour(F.col("last_event_at")).between(10, 16))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "ticket_priority",
        "ticket_closed_at",
        "next3_event_at",
        "last_event_at",
        "prev_event_at",
        "event_at_decile",
        "event_at_dense_rank",
        "event_at_decile_filled",
        F.date_trunc("hour", F.col("next3_event_at")).alias("next3_event_at_hour"),
    )
)

result = (
    tagged
    .groupingSets(
        [["event_at_dense_rank", "event_at_decile_filled"], ["event_at_dense_rank"]],
        "event_at_dense_rank", "event_at_decile_filled",
    )
    .agg(F.count("*").alias("row_count"))
)
