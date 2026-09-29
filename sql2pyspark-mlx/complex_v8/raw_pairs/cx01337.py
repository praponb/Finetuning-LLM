from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_quarter_year_by_eve = Window.partitionBy(F.year("event_at_quarter")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at_quarter").asc_nulls_last())
w_second_event_at_month_by_eve = Window.partitionBy(F.month("second_event_at")).orderBy("event_at_quarter")

filtered = (
    df_ticket_events
    .filter(F.col("actor") != "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.last("event_at_quarter").over(w_event_at_quarter_year_by_eve).alias("last_event_at_quarter"),
        F.nth_value("event_at", 2).over(w_event_type_ticket_id_by_even).alias("second_event_at"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.round(F.cume_dist().over(w_ticket_id_actor_by_event_at), 3).alias("event_at_quarter_cume_dist"),
        F.round(F.cume_dist().over(w_second_event_at_month_by_eve), 3).alias("event_at_quarter_cume_dist_2"),
    )
)

result = (
    windowed
    .filter(F.col("event_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_quarter",
        "last_event_at_quarter",
        "second_event_at",
        "event_at_quarter_cume_dist",
    )
)
