from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at_percent_rank").desc(), "id")
w_event_type_event_at_month = Window.partitionBy("event_type", F.month("event_at"))

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

with_windows = (
    prepared_events
    .select(
        "*",
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.last("event_at").over(w_event_type_ticket_id_by_even).alias("last_event_at"),
        F.nth_value("event_at", 2).over(w_event_type_ticket_id_by_even).alias("second_event_at"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_prefix",
        "event_type_slug",
        "event_at_label",
        "event_at_percent_rank",
        "second_event_at",
        F.round((F.unix_timestamp("second_event_at") - F.unix_timestamp(F.lag("second_event_at").over(w_event_type_ticket_id_by_even_2))) / 60, 1).alias("minutes_since_prev_second_event_at"),
        F.size(F.collect_set("actor").over(w_event_type_event_at_month)).alias("distinct_actor_in_event_type_event_at_month"),
    )
)

result = (
    sequenced
    .select("id", "event_at_percent_rank", "minutes_since_prev_second_event_at")
    .unpivot(
        ids=["id"],
        values=["event_at_percent_rank", "minutes_since_prev_second_event_at"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
