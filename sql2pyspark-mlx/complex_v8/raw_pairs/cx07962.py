from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prev_event_at = Window.orderBy(F.col("prev_event_at").desc(), "id")

base = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.last("event_at").over(w_actor_by_event_at_full).alias("last_event_at"),
        F.lag("event_at").over(w_event_type_by_event_at).alias("prev_event_at"),
        F.row_number().over(w_event_type_by_event_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_actor_by_event_at), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.lag("event_at", 3).over(w_event_type_by_event_at).alias("prev3_event_at"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id").between(7, 19)) & (F.hour(F.col("t.opened_at")).between(7, 12)), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at_2"),
        F.max("event_at").alias("last_event_at_3"),
        F.min("event_at").alias("first_event_at"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at_2",
        "t.last_event_at_3",
        "t.first_event_at",
    )
)

sequenced_2 = (
    with_stats
    .select(
        "*",
        F.round((F.unix_timestamp("second_event_at") - F.unix_timestamp(F.lag("second_event_at").over(w_all_by_prev_event_at))) / 60, 1).alias("minutes_since_prev_second_event_at"),
        F.ntile(10).over(w_all_by_prev_event_at).alias("prev_event_at_decile"),
        F.lag("seq_no", 2, 0).over(w_all_by_prev_event_at).alias("prev2_seq_no"),
    )
)

with_details_ticket = (
    sequenced_2.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.priority").alias("ticket_priority"))
)

scored = (
    with_details_ticket
    .filter(F.col("second_event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "last_event_at",
        "prev_event_at",
        "seq_no",
        "prev3_event_at",
        "last_event_at_2",
        "first_event_at",
        "minutes_since_prev_second_event_at",
        "prev_event_at_decile",
        "ticket_priority",
        F.length(F.trim(F.col("actor_event_type_label"))).alias("actor_event_type_label_len"),
        (F.floor(F.col("seq_no") / 2) * 2).alias("seq_no_bucket"),
        F.abs(F.col("event_at_cume_dist") - 0.6).alias("event_at_cume_dist_distance"),
    )
)

result = (
    scored
    .select("id", "minutes_since_prev_second_event_at", "event_at_cume_dist_distance")
    .unpivot(
        ids=["id"],
        values=["minutes_since_prev_second_event_at", "event_at_cume_dist_distance"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
