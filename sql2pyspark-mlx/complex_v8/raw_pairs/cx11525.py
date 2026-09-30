from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_closed_at = Window.orderBy(F.col("ticket_closed_at").desc(), "id")
w_all_by_ticket_closed_at_full = Window.orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.days_since_event_at",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

scored = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "ticket_closed_at",
        "ticket_opened_at",
        F.when(F.col("days_since_event_at") >= 420, "large").when(F.col("days_since_event_at") >= 140, "medium").otherwise("small").alias("days_since_event_at_tier"),
    )
)

windowed = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_at",
        "ticket_closed_at",
        "ticket_opened_at",
        F.lag("ticket_closed_at", 2).over(w_all_by_ticket_closed_at).alias("prev2_ticket_closed_at"),
        F.ntile(5).over(w_all_by_ticket_closed_at).alias("ticket_closed_at_quintile"),
        F.row_number().over(w_all_by_ticket_closed_at).alias("rn"),
        F.last("ticket_opened_at").over(w_all_by_ticket_closed_at_full).alias("last_ticket_opened_at"),
    )
)

enriched = (
    windowed
    .select(
        "id",
        "event_at",
        "ticket_closed_at",
        "ticket_opened_at",
        "ticket_closed_at_quintile",
        "rn",
        F.month(F.col("ticket_opened_at")).alias("ticket_opened_at_month"),
        F.to_date(F.col("prev2_ticket_closed_at")).alias("prev2_ticket_closed_at_date"),
    )
)

filtered_2 = (
    enriched
    .crossJoin(enriched.agg(F.max("ticket_closed_at_quintile").alias("overall_max_ticket_closed_at_quintile")))
    .filter(F.col("ticket_closed_at_quintile") < F.col("overall_max_ticket_closed_at_quintile"))
    .drop("overall_max_ticket_closed_at_quintile")
)

without_matches = (
    filtered_2
    .crossJoin(filtered_2.agg(F.max("ticket_opened_at_month").alias("overall_max_ticket_opened_at_month")))
    .filter(F.col("ticket_opened_at_month") < F.col("overall_max_ticket_opened_at_month"))
    .drop("overall_max_ticket_opened_at_month")
)

result = (
    without_matches
    .select(
        "id",
        "event_at",
        "ticket_closed_at",
        "ticket_closed_at_quintile",
        "rn",
        "ticket_opened_at_month",
        "prev2_ticket_closed_at_date",
        F.date_trunc("year", F.col("ticket_closed_at")).alias("ticket_closed_at_year"),
    )
    .orderBy(
        F.col("ticket_closed_at_quintile").asc(),
        F.col("ticket_opened_at_month").desc(),
    )
)
