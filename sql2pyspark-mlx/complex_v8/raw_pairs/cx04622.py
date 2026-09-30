from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_opened_at = Window.orderBy("ticket_opened_at", "id")
w_event_at_to_date_by_ticket_o = Window.partitionBy(F.to_date("event_at")).orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_by_ticket_o_2 = Window.partitionBy(F.to_date("event_at")).orderBy("ticket_opened_at", "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc())
w_event_at_to_date_by_ticket_o_3 = Window.partitionBy(F.to_date("event_at")).orderBy("ticket_opened_at")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")

recent = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_event_type_label",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.ntile(4).over(w_all_by_ticket_opened_at).alias("ticket_opened_at_quartile"),
        F.nth_value("event_at", 2).over(w_event_at_to_date_by_ticket_o).alias("second_event_at"),
        F.ntile(5).over(w_event_at_to_date_by_ticket_o_2).alias("ticket_opened_at_quintile"),
        F.rank().over(w_event_type_by_event_at_ties).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_event_at_to_date_by_ticket_o_3), 3).alias("ticket_opened_at_cume_dist"),
        F.rank().over(w_all_by_ticket_opened_at).alias("ticket_opened_at_rank"),
        F.dense_rank().over(w_event_type_by_event_at).alias("event_at_dense_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "actor_event_type_label",
        "ticket_tags",
        "ticket_opened_at_quintile",
        "event_at_rank",
        "event_at_dense_rank",
        (F.floor(F.col("ticket_opened_at_rank") / 2) * 2).alias("ticket_opened_at_rank_bucket"),
        F.date_trunc("week", F.col("second_event_at")).alias("second_event_at_week"),
        F.round((F.unix_timestamp(F.col("second_event_at")) - F.unix_timestamp(F.col("ticket_opened_at"))) / 3600, 1).alias("hours_ticket_opened_at_to_second_event_at"),
    )
)

result = (
    enriched
    .select(
        "id",
        "actor_event_type_label",
        "ticket_opened_at_quintile",
        "event_at_rank",
        "event_at_dense_rank",
        "ticket_opened_at_rank_bucket",
        "hours_ticket_opened_at_to_second_event_at",
        F.abs(F.col("event_at_dense_rank") - 7).alias("event_at_dense_rank_distance"),
    )
    .orderBy(F.col("actor_event_type_label").desc(), "id")
    .limit(10)
)
