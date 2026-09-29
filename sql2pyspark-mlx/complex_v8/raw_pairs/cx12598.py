from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_event_at = Window.partitionBy(F.month("event_at")).orderBy("event_at", "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_event_at_t = Window.partitionBy(F.month("event_at")).orderBy("event_at")
w_last_event_at_month_event_at = Window.partitionBy(F.month("last_event_at"), "event_at_rank").orderBy("minutes_since_prev_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_event_at_month_event_at_2 = Window.partitionBy(F.month("last_event_at"), "event_at_rank").orderBy("minutes_since_prev_event_at", "id")

base = (
    df_ticket_events
    .filter((F.col("event_type").isin("assigned", "resolved")) & (F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    base
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.last("event_at").over(w_event_type_by_event_at_full).alias("last_event_at"),
        F.ntile(10).over(w_event_at_month_by_event_at).alias("event_at_decile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(10).over(w_event_type_by_event_at).alias("event_at_decile_2"),
        F.row_number().over(w_event_type_by_event_at).alias("event_at_seq"),
        F.rank().over(w_event_at_month_by_event_at_t).alias("event_at_rank"),
    )
)

qualified = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") >= 1) & (F.col("t.priority") == "P4"), "left_semi")
)

ranked = (
    qualified
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "last_event_at",
        "event_at_decile",
        "minutes_since_prev_event_at",
        "event_at_decile_2",
        "event_at_seq",
        "event_at_rank",
        F.last("minutes_since_prev_event_at").over(w_last_event_at_month_event_at).alias("last_minutes_since_prev_event_at"),
        F.lead("event_at_decile_2").over(w_last_event_at_month_event_at_2).alias("next_event_at_decile_2"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "last_event_at",
        "event_at_decile",
        "minutes_since_prev_event_at",
        "event_at_decile_2",
        "event_at_seq",
        "event_at_rank",
        "last_minutes_since_prev_event_at",
        "next_event_at_decile_2",
        F.greatest(F.col("event_at_rank"), F.col("next_event_at_decile_2")).alias("greatest_event_at_rank_next_event_at_decile_2"),
    )
    .orderBy(F.col("event_at_seq").desc(), F.col("event_at_rank").desc_nulls_first())
)
