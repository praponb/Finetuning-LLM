from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_at_to_date_ticket_id_b = Window.partitionBy(F.to_date("event_at"), "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_event_at_to_date_ticket_id_b_2 = Window.partitionBy(F.to_date("event_at"), "ticket_id").orderBy(F.col("event_at").desc())
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

with_trends = (
    valid_events
    .select(
        "*",
        F.first("event_at").over(w_actor_event_type_by_event_at).alias("first_event_at"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_to_date_ticket_id_b))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_event_at_to_date_ticket_id_b_2), 3).alias("event_at_percent_rank"),
        F.row_number().over(w_event_type_ticket_id_by_even).alias("rn"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
    )
)

enriched = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_quintile",
        "minutes_since_prev_event_at",
        "rn",
        "event_at_dense_rank",
        "last_event_at",
        F.round(F.col("rn") / F.nullif(F.col("event_at_percent_rank"), F.lit(0)), 3).alias("rn_per_event_at_percent_rank"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

result = (
    enriched
    .filter(F.col("event_at_dense_rank").between(3, 10))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_quintile",
        "event_at_dense_rank",
        "last_event_at",
        "rn_per_event_at_percent_rank",
        "event_at_date",
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("last_event_at"))) / 3600, 1).alias("hours_last_event_at_to_event_at"),
        F.month(F.col("last_event_at")).alias("last_event_at_month"),
    )
)
