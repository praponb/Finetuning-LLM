from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_last_event_at_to = Window.partitionBy("event_type", F.to_date("last_event_at")).orderBy(F.col("last_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_last_event_at_to_2 = Window.partitionBy("event_type", F.to_date("last_event_at")).orderBy(F.col("last_event_at").desc(), "id")
w_event_at_year = Window.partitionBy(F.year("event_at"))
w_last_event_at_year_by_event = Window.partitionBy(F.year("last_event_at")).orderBy("event_at", "id")
w_all_by_second_last_event_at = Window.orderBy(F.col("second_last_event_at").desc(), "id").rowsBetween(-1, -1)
w_all_by_second_last_event_at_2 = Window.orderBy(F.col("second_last_event_at").desc())

base_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

filtered = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.month(F.col("t.closed_at")).isin(2, 3)), "left_anti")
)

filtered_2 = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel") != "web") & (F.col("t.requester_id") < 18), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

summarized_rows = (
    filtered_2.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.nth_value("last_event_at", 2).over(w_event_type_last_event_at_to).alias("second_last_event_at"),
        F.lead("last_event_at").over(w_event_type_last_event_at_to_2).alias("next_last_event_at"),
        F.lag("last_event_at").over(w_event_type_last_event_at_to_2).alias("prev_last_event_at"),
        F.round(F.avg("event_count").over(w_event_at_year), 2).alias("event_at_year_avg_event_count"),
        F.row_number().over(w_event_type_last_event_at_to_2).alias("last_event_at_seq"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.lag("prev_last_event_at", 2).over(w_last_event_at_year_by_event).alias("prev2_prev_last_event_at"),
        F.round(F.avg("event_count").over(w_all_by_second_last_event_at), 2).alias("event_count_avg_1_prior"),
        F.round(F.cume_dist().over(w_all_by_second_last_event_at_2), 3).alias("second_last_event_at_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "first_event_at",
        "event_count",
        "second_last_event_at",
        "prev_last_event_at",
        "last_event_at_seq",
        "prev2_prev_last_event_at",
        "event_count_avg_1_prior",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.substring(F.trim(F.col("event_type_ref")), 1, 3).alias("event_type_ref_prefix"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
