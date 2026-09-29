from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_ticket_id_event_at_month_by = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id")
w_ticket_id_event_at_month_by_2 = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_at_month_by_3 = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_last_known_even = Window.partitionBy("ticket_id").orderBy(F.col("last_known_event_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_by_last_known_even_2 = Window.partitionBy("ticket_id").orderBy(F.col("last_known_event_at").desc(), "id")
w_ticket_id_by_last_known_even_3 = Window.partitionBy("ticket_id").orderBy(F.col("last_known_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor = Window.partitionBy("actor")

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.when(F.col("event_type").isin("escalated", "reopened"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
    )
)

labeled = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_slug")), " ", "_")).alias("event_type_slug_slug"),
        F.when(F.col("event_type_group").isin("other", "group_a"), "group_a").when(F.col("event_type_group") == "group_b", "group_b").otherwise("other").alias("event_type_group_group"),
    )
)

with_activity = (
    labeled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(10, 15)) & (~F.col("t.event_type").isin("escalated", "resolved")), "left_anti")
)

with_trends = (
    with_activity
    .select(
        "*",
        F.lead("event_at", 3).over(w_ticket_id_event_type_by_even).alias("next3_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_event_at_month_by), 3).alias("event_at_cume_dist"),
        F.first("event_at").over(w_ticket_id_event_at_month_by_2).alias("first_event_at"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_event_at_month_by_3).alias("last_known_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_event_type_by_even_2).alias("second_event_at"),
    )
)

ranked = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group_group",
        "event_at_cume_dist",
        F.round(F.avg("event_at_cume_dist").over(w_ticket_id_by_last_known_even), 2).alias("event_at_cume_dist_avg_7"),
        F.lag("event_at", 3).over(w_ticket_id_by_last_known_even_2).alias("prev3_event_at"),
        F.first("last_known_event_at").over(w_ticket_id_by_last_known_even_3).alias("first_last_known_event_at"),
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.sum("event_at_cume_dist").over(w_actor).alias("actor_sum_event_at_cume_dist"),
        F.max("event_at_cume_dist").over(w_actor).alias("actor_max_event_at_cume_dist"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.coalesce(F.col("event_type_group_group"), F.lit("UNKNOWN")).alias("event_type_group_group_or_unknown"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.least(F.col("event_at_cume_dist"), F.col("rows_in_actor")).alias("least_event_at_cume_dist_rows_in_actor"),
    )
)

joined_ticket = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_group_group",
        "s.event_at_cume_dist_avg_7",
        "s.prev3_event_at",
        "s.first_last_known_event_at",
        "s.rows_in_actor",
        "s.actor_sum_event_at_cume_dist",
        "s.actor_max_event_at_cume_dist",
        "s.event_type_group_group_or_unknown",
        "s.event_type_ref",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group_group",
        "prev3_event_at",
        "first_last_known_event_at",
        "rows_in_actor",
        "actor_sum_event_at_cume_dist",
        "actor_max_event_at_cume_dist",
        "event_type_group_group_or_unknown",
        "event_type_ref",
    )
    .orderBy(F.col("event_type").asc_nulls_last(), "id")
    .limit(5)
)
