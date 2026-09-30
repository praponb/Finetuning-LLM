from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_days_since_eve = Window.partitionBy("event_type").orderBy("days_since_event_at", "id").rowsBetween(-3, -1)
w_ticket_id_event_at_month_by = Window.partitionBy("ticket_id", F.month("event_at")).orderBy(F.col("event_at").desc_nulls_first(), "id")
w_ticket_id_event_at_month_by_2 = Window.partitionBy("ticket_id", F.month("event_at")).orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_days_since_eve_2 = Window.partitionBy("event_type").orderBy("days_since_event_at", "id")
w_event_type_event_at_hour_yea = Window.partitionBy("event_type", F.year("event_at_hour"))
w_event_type_by_days_since_eve_3 = Window.partitionBy("event_type").orderBy("days_since_event_at")

valid = (
    df_ticket_events
    .filter((F.col("event_type") == "resolved") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

screened = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.event_type").isin("assigned", "escalated")) & (F.month(F.col("t.event_at")).isin(1, 2)), "left_anti")
)

windowed = (
    screened
    .select(
        "*",
        F.round(F.avg("days_since_event_at").over(w_event_type_by_days_since_eve), 2).alias("days_since_event_at_avg_3_prior"),
        F.ntile(10).over(w_ticket_id_event_at_month_by).alias("event_at_decile"),
        F.nth_value("event_at_hour", 2).over(w_ticket_id_event_at_month_by_2).alias("second_event_at_hour"),
        F.rank().over(w_event_type_by_days_since_eve_2).alias("days_since_event_at_rank"),
        F.count(F.lit(1)).over(w_event_type_event_at_hour_yea).alias("rows_in_event_type_event_at_hour_year"),
        F.round(F.percent_rank().over(w_event_type_by_days_since_eve_3), 3).alias("days_since_event_at_percent_rank"),
    )
)

result = (
    windowed
    .filter(F.col("event_at_decile").between(1, 8))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        "days_since_event_at",
        "days_since_event_at_avg_3_prior",
        "event_at_decile",
        "second_event_at_hour",
        "days_since_event_at_rank",
        "days_since_event_at_percent_rank",
        F.concat_ws(" / ", F.col("actor_slug"), F.col("actor")).alias("actor_slug_actor_label"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("ticket_id").asc_nulls_last())
)
