from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_prev_event_at = Window.partitionBy("event_type").orderBy("prev_event_at")
w_event_type_seq_no = Window.partitionBy("event_type", "seq_no")
w_seq_no_2_next_event_at_month = Window.partitionBy("seq_no_2", F.month("next_event_at")).orderBy(F.col("last_known_event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_first_event_at_full = Window.orderBy(F.col("first_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_events = (
    df_ticket_events
    .filter((~F.col("actor").isin("system", "customer")) | (F.col("event_type").isin("reopened", "created")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

ranked = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_ref",
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_actor_by_event_at).alias("seq_no_2"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.lead("event_at").over(w_ticket_id_by_event_at).alias("next_event_at_2"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
    )
)

enriched = (
    ranked
    .filter(F.col("next_event_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_ref")), " ", "_")).alias("event_type_ref_slug"),
        F.when(F.col("seq_no") >= 6, "large").when(F.col("seq_no") >= 4, "medium").otherwise("small").alias("seq_no_tier"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_type_by_prev_event_at), 3).alias("prev_event_at_percent_rank"),
        F.max("seq_no").over(w_event_type_seq_no).alias("event_type_seq_no_max_seq_no"),
        F.sum("seq_no_2").over(w_seq_no_2_next_event_at_month).alias("running_seq_no_2"),
        F.nth_value("last_known_event_at", 2).over(w_all_by_first_event_at_full).alias("second_last_known_event_at"),
        F.max("event_at_rank").over(w_event_type_seq_no).alias("event_type_seq_no_max_event_at_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_ref",
        "event_at_rank",
        "seq_no",
        "prev_event_at",
        "seq_no_2",
        "next_event_at",
        "next_event_at_2",
        "first_event_at",
        "last_known_event_at",
        "event_type_ref_slug",
        "seq_no_tier",
        "prev_event_at_percent_rank",
        "event_type_seq_no_max_seq_no",
        "running_seq_no_2",
        "event_type_seq_no_max_event_at_rank",
        F.coalesce(F.col("running_seq_no_2"), F.lit(0)).alias("running_seq_no_2_filled"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
    .orderBy(F.col("running_seq_no_2_filled").desc())
)
