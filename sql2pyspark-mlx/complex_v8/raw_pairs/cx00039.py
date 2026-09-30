from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last())
w_ticket_id_event_type_by_even_3 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_month_event_type_by = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even_4 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_event_at_month_event_type_by_2 = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type").orderBy("event_at", "id")
w_event_at_month_by_event_at_f = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even_5 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

metrics = (
    base_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_ref",
        F.nth_value("event_at", 2).over(w_ticket_id_event_type_by_even).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("event_at").over(w_ticket_id_event_type_by_even).alias("last_event_at"),
        F.rank().over(w_ticket_id_event_type_by_even_2).alias("event_at_rank"),
        F.ntile(4).over(w_ticket_id_event_type_by_even_3).alias("event_at_quartile"),
        F.last("event_at", ignorenulls=True).over(w_event_at_month_event_type_by).alias("last_known_event_at"),
        F.lag("event_at", 3).over(w_ticket_id_event_type_by_even_4).alias("prev3_event_at"),
        F.dense_rank().over(w_ticket_id_event_type_by_even_2).alias("event_at_dense_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_event_type_by_2))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.nth_value("event_at", 2).over(w_event_at_month_by_event_at_f).alias("second_event_at_2"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_event_type_by_even_5).alias("last_known_event_at_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "second_event_at",
        "minutes_since_prev_event_at",
        "last_event_at",
        "event_at_rank",
        "event_at_quartile",
        "last_known_event_at",
        "event_at_dense_rank",
        "minutes_since_prev_event_at_2",
        "second_event_at_2",
        "last_known_event_at_2",
        F.coalesce(F.col("event_at_quartile"), F.lit(0)).alias("event_at_quartile_filled"),
        F.abs(F.col("event_at_rank") - 5).alias("event_at_rank_distance"),
    )
    .orderBy(F.col("ticket_id").asc())
)
