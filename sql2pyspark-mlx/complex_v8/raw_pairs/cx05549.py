from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_all_by_ticket_closed_at = Window.orderBy(F.col("ticket_closed_at").desc_nulls_first(), "id")
w_ticket_closed_at_year_by_eve = Window.partitionBy(F.year("ticket_closed_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_closed_at_year_by_eve_2 = Window.partitionBy(F.year("ticket_closed_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_closed_at_year_by_eve_3 = Window.partitionBy(F.year("ticket_closed_at")).orderBy(F.col("event_at").desc(), "id")
w_all_by_ticket_closed_at_full = Window.orderBy(F.col("ticket_closed_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_event_at_week_ru = Window.orderBy(F.col("next_event_at_week").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_at_week_to_date = Window.partitionBy("actor", F.to_date("event_at_week")).orderBy("minutes_since_prev_event_at_week", "id")
w_next_event_at_week_month = Window.partitionBy(F.date_trunc("month", "next_event_at_week"))
w_all_by_next_event_at_week = Window.orderBy(F.col("next_event_at_week").desc(), "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")

prepared_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

combined_details = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.closed_at").alias("ticket_closed_at"))
)

sequenced = (
    combined_details
    .select(
        "*",
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_ticket_closed_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("ticket_closed_at").over(w_ticket_closed_at_year_by_eve).alias("last_ticket_closed_at"),
        F.round((F.unix_timestamp("event_at_week") - F.unix_timestamp(F.lag("event_at_week").over(w_all_by_ticket_closed_at))) / 60, 1).alias("minutes_since_prev_event_at_week"),
        F.row_number().over(w_all_by_ticket_closed_at).alias("rn"),
        F.first("event_at_week", ignorenulls=True).over(w_ticket_closed_at_year_by_eve_2).alias("first_known_event_at_week"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.lead("event_at_week").over(w_ticket_closed_at_year_by_eve_3).alias("next_event_at_week"),
        F.first("event_at").over(w_all_by_ticket_closed_at_full).alias("first_event_at"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_or_unknown",
        "event_at_week",
        "minutes_since_prev_event_at",
        "minutes_since_prev_event_at_week",
        "rn",
        F.first("event_at", ignorenulls=True).over(w_all_by_next_event_at_week_ru).alias("first_known_event_at"),
        F.row_number().over(w_actor_event_at_week_to_date).alias("rn_2"),
        F.size(F.collect_set("event_type").over(w_next_event_at_week_month)).alias("distinct_event_type_in_next_event_at_week_month"),
        F.max("event_at_cume_dist").over(w_next_event_at_week_month).alias("next_event_at_week_month_max_event_at_cume_dist"),
        F.rank().over(w_all_by_next_event_at_week).alias("next_event_at_week_rank"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
    )
)

result = (
    metrics
    .filter(F.col("actor").isin("agent", "customer"))
    .select(
        "id",
        "actor",
        "event_at_week",
        "minutes_since_prev_event_at",
        "rn",
        "first_known_event_at",
        "rn_2",
        "distinct_event_type_in_next_event_at_week_month",
        "next_event_at_week_month_max_event_at_cume_dist",
        "event_at_quintile",
        F.coalesce(F.col("event_at_quintile"), F.lit(0)).alias("event_at_quintile_filled"),
    )
    .orderBy(
        F.col("next_event_at_week_month_max_event_at_cume_dist").desc_nulls_first(),
    )
)
