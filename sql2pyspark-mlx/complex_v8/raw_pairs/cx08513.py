from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_to_date_by_ev = Window.partitionBy(F.to_date("event_at_month")).orderBy("event_at", "id")
w_ticket_id_by_event_at_month = Window.partitionBy("ticket_id").orderBy(F.col("event_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_month_to_date_by_ev_2 = Window.partitionBy(F.to_date("event_at_month")).orderBy("event_at")
w_event_at_month_to_date_ticke = Window.partitionBy(F.to_date("event_at_month"), "ticket_id").orderBy("event_at_month")
w_event_at_month_to_date_ticke_2 = Window.partitionBy(F.to_date("event_at_month"), "ticket_id").orderBy("event_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_month_2 = Window.partitionBy("ticket_id").orderBy(F.col("event_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_month_to_date_ticke_3 = Window.partitionBy(F.to_date("event_at_month"), "ticket_id").orderBy("event_at_month", "id")

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

sequenced = (
    cleaned_events
    .select(
        "*",
        F.row_number().over(w_event_at_month_to_date_by_ev).alias("event_at_seq"),
        F.first("event_at_month", ignorenulls=True).over(w_ticket_id_by_event_at_month).alias("first_known_event_at_month"),
        F.round(F.cume_dist().over(w_event_at_month_to_date_by_ev_2), 3).alias("event_at_cume_dist"),
        F.round(F.percent_rank().over(w_event_at_month_to_date_ticke), 3).alias("event_at_month_percent_rank"),
        F.first("event_at").over(w_event_at_month_to_date_ticke_2).alias("first_event_at"),
        F.nth_value("event_at_month", 2).over(w_ticket_id_by_event_at_month_2).alias("second_event_at_month"),
        F.last("event_at_month", ignorenulls=True).over(w_event_type_actor_by_event_at).alias("last_known_event_at_month"),
        F.round((F.unix_timestamp("event_at_month") - F.unix_timestamp(F.lag("event_at_month").over(w_event_at_month_to_date_ticke_3))) / 60, 1).alias("minutes_since_prev_event_at_month"),
    )
)

first_rows = (
    sequenced
    .filter(F.col("event_at_seq") == 1)
    .drop("event_at_seq")
)

result = (
    first_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_month",
        "actor_event_type_label",
        "first_known_event_at_month",
        "event_at_month_percent_rank",
        "first_event_at",
        "minutes_since_prev_event_at_month",
        F.round((F.unix_timestamp(F.col("event_at_month")) - F.unix_timestamp(F.col("second_event_at_month"))) / 3600, 1).alias("hours_second_event_at_month_to_event_at_month"),
        F.when(F.col("event_type").isin("created", "escalated"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
    )
    .orderBy(F.col("event_at_month").desc_nulls_first(), "id")
    .limit(20)
)
