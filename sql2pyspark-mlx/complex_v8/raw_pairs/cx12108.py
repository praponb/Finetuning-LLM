from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id")
w_actor_or_unknown_ticket_id_b = Window.partitionBy("actor_or_unknown", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_or_unknown_ticket_id_b_2 = Window.partitionBy("actor_or_unknown", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_year_year_by_event = Window.partitionBy("event_at_year_year").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_event_at_year_month = Window.partitionBy(F.date_trunc("month", "event_at_year"))
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_year_year_by_event_2 = Window.partitionBy("event_at_year_year").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_ticket_events
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

tagged = (
    prepared
    .select(
        "*",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.year(F.col("event_at_year")).alias("event_at_year_year"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.round(F.col("event_at_year_year") - F.avg("event_at_year_year").over(w_ticket_id), 2).alias("event_at_year_year_vs_ticket_id_avg"),
        F.first("event_at_year_year", ignorenulls=True).over(w_actor_or_unknown_ticket_id_b).alias("first_known_event_at_year_year"),
        F.lead("event_at_year_year", 3).over(w_actor_or_unknown_ticket_id_b_2).alias("next3_event_at_year_year"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.lag("event_at").over(w_event_at_year_year_by_event).alias("prev_event_at"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.count(F.lit(1)).over(w_event_at_year_month).alias("rows_in_event_at_year_month"),
        F.first("event_at_year_year", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at_year_year_2"),
        F.sum("event_at_year_year").over(w_event_at_year_year_by_event_2).alias("event_at_year_year_sum_3"),
    )
)

qualified = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.hour(F.col("t.opened_at")).between(8, 13)) & (~F.col("t.channel").isin("phone", "chat")), "left_anti")
)

with_parent = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.opened_at").alias("ticket_opened_at"))
)

result = (
    with_parent
    .groupBy("first_known_event_at_year_year_2", "event_at_year")
    .pivot("actor", ["agent", "customer"])
    .agg(F.avg("event_at_year_year_sum_3"))
    .orderBy("first_known_event_at_year_year_2", "event_at_year")
)
