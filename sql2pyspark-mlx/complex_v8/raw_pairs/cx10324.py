from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc())
w_all_by_last_event_at_running = Window.orderBy("last_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_event_at = Window.orderBy("last_event_at", "id")
w_event_type = Window.partitionBy("event_type")
w_all_by_last_event_at_prior_1 = Window.orderBy("last_event_at", "id").rowsBetween(-1, -1)

base_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_parent = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.array_contains(F.col("t.tags"), "refund"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_prefix",
        F.col("t.tags").alias("ticket_tags"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_prefix",
        "ticket_tags",
        F.row_number().over(w_actor_by_event_at).alias("event_at_seq"),
        F.round(F.percent_rank().over(w_actor_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_event_type_actor_by_event_at).alias("event_at_rank"),
        F.lead("event_at").over(w_event_type_actor_by_event_at).alias("next_event_at"),
        F.round(F.percent_rank().over(w_event_type_actor_by_event_at_2), 3).alias("event_at_percent_rank_2"),
        F.row_number().over(w_event_type_actor_by_event_at).alias("seq_no"),
        F.last("event_at").over(w_actor_by_event_at_full).alias("last_event_at"),
    )
)

top_per_group = (
    metrics
    .filter((F.col("seq_no") == 1) & (F.hour(F.col("last_event_at")).between(8, 13)))
    .drop("seq_no")
)

sequenced = (
    top_per_group
    .select(
        "id",
        "event_at",
        "ticket_tags",
        "event_at_seq",
        "second_event_at",
        "next_event_at",
        "event_at_percent_rank_2",
        "last_event_at",
        F.last("next_event_at", ignorenulls=True).over(w_all_by_last_event_at_running).alias("last_known_next_event_at"),
        F.lead("event_at_percent_rank", 3).over(w_all_by_last_event_at).alias("next3_event_at_percent_rank"),
        F.round(F.avg("event_at_percent_rank_2").over(w_event_type), 2).alias("event_type_avg_event_at_percent_rank_2"),
        F.round(F.avg("event_at_seq").over(w_all_by_last_event_at_prior_1), 2).alias("event_at_seq_avg_1_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_at",
        "ticket_tags",
        "event_at_seq",
        "second_event_at",
        "next_event_at",
        "event_at_percent_rank_2",
        "last_event_at",
        "last_known_next_event_at",
        "next3_event_at_percent_rank",
    )
    .orderBy(
        F.col("next_event_at").desc(),
        F.col("next3_event_at_percent_rank").desc_nulls_first(),
    )
)
