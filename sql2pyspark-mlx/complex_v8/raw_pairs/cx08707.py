from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_ticket_opened_at_ru = Window.partitionBy("actor").orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_ticket_opened_at = Window.partitionBy("actor").orderBy("ticket_opened_at", "id")
w_ticket_opened_at_to_date_tic = Window.partitionBy(F.to_date("ticket_opened_at"), F.date_trunc("month", "ticket_closed_at")).orderBy(F.col("last_known_ticket_opened_at").asc_nulls_last(), "id")
w_ticket_opened_at_to_date_tic_2 = Window.partitionBy(F.to_date("ticket_opened_at"), F.date_trunc("month", "ticket_closed_at")).orderBy(F.col("last_known_ticket_opened_at").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_ticket_closed_at_ties = Window.orderBy(F.col("ticket_closed_at").desc())
w_ticket_closed_at_year_next_e = Window.partitionBy(F.year("ticket_closed_at"), F.month("next_event_at"))
w_all_by_ticket_closed_at = Window.orderBy(F.col("ticket_closed_at").desc(), "id")
w_ticket_closed_at_year_ticket = Window.partitionBy(F.year("ticket_closed_at"), "ticket_opened_at_tercile")
w_all_by_ticket_closed_at_cume = Window.orderBy("ticket_closed_at_cume_dist", "id")
w_ticket_opened_at_tercile_dis = Window.partitionBy("ticket_opened_at_tercile_distance").orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")) & (F.col("event_type").isin("reopened", "escalated", "resolved")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

ranked = (
    combined_details
    .select(
        "id",
        "event_at",
        "event_type",
        "ticket_closed_at",
        "ticket_opened_at",
        F.last("ticket_opened_at", ignorenulls=True).over(w_actor_by_ticket_opened_at_ru).alias("last_known_ticket_opened_at"),
        F.ntile(3).over(w_actor_by_ticket_opened_at).alias("ticket_opened_at_tercile"),
        F.lag("event_at").over(w_actor_by_ticket_opened_at).alias("prev_event_at"),
        F.lead("event_at").over(w_actor_by_ticket_opened_at).alias("next_event_at"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "event_type",
        "ticket_closed_at",
        "last_known_ticket_opened_at",
        "ticket_opened_at_tercile",
        "prev_event_at",
        F.round(F.cume_dist().over(w_ticket_opened_at_to_date_tic), 3).alias("last_known_ticket_opened_at_cume_dist"),
        F.round(F.avg("ticket_opened_at_tercile").over(w_ticket_opened_at_to_date_tic_2), 2).alias("ticket_opened_at_tercile_avg_3"),
        F.round(F.percent_rank().over(w_all_by_ticket_closed_at_ties), 3).alias("ticket_closed_at_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_ticket_closed_at_ties), 3).alias("ticket_closed_at_cume_dist"),
        F.round(F.col("ticket_opened_at_tercile") / F.nullif(F.sum("ticket_opened_at_tercile").over(w_ticket_closed_at_year_next_e), F.lit(0)), 4).alias("ticket_opened_at_tercile_share_of_ticket_closed_"),
        (F.col("ticket_opened_at_tercile") - F.lag("ticket_opened_at_tercile").over(w_all_by_ticket_closed_at)).alias("ticket_opened_at_tercile_change"),
    )
)

enriched = (
    windowed
    .select(
        "id",
        "ticket_closed_at",
        "last_known_ticket_opened_at",
        "ticket_opened_at_tercile",
        "prev_event_at",
        "last_known_ticket_opened_at_cume_dist",
        "ticket_opened_at_tercile_avg_3",
        "ticket_closed_at_cume_dist",
        "ticket_opened_at_tercile_share_of_ticket_closed_",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.abs(F.col("ticket_opened_at_tercile") - 5).alias("ticket_opened_at_tercile_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round((F.unix_timestamp("prev_event_at") - F.unix_timestamp(F.lag("prev_event_at").over(w_all_by_ticket_closed_at_cume))) / 60, 1).alias("minutes_since_prev_prev_event_at"),
        F.ntile(4).over(w_all_by_ticket_closed_at_cume).alias("ticket_closed_at_cume_dist_quartile"),
        F.nth_value("ticket_closed_at", 2).over(w_ticket_opened_at_tercile_dis).alias("second_ticket_closed_at"),
        F.count(F.lit(1)).over(w_ticket_closed_at_year_ticket).alias("rows_in_ticket_closed_at_year_ticket_opened_at_t"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "ticket_opened_at_tercile_distance",
        "event_type_len",
        "ticket_opened_at_tercile",
        "ticket_closed_at_cume_dist_quartile",
    )
    .unpivot(
        ids=["id"],
        values=["ticket_opened_at_tercile_distance", "event_type_len", "ticket_opened_at_tercile", "ticket_closed_at_cume_dist_quartile"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
