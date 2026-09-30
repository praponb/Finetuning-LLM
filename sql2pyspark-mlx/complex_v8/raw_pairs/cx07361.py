from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_by_event_at = Window.partitionBy("ticket_priority").orderBy("event_at")
w_ticket_priority_ticket_id_by = Window.partitionBy("ticket_priority", "ticket_id").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_ticket_opened_at = Window.partitionBy("actor").orderBy(F.col("ticket_opened_at").desc(), "id")
w_ticket_priority_ticket_id_by_2 = Window.partitionBy("ticket_priority", "ticket_id").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_priority_by_event_at_2 = Window.partitionBy("ticket_priority").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_priority_by_event_at_3 = Window.partitionBy("ticket_priority").orderBy("event_at", "id")
w_row_count_by_avg_minutes_sin = Window.partitionBy("row_count").orderBy("avg_minutes_since_prev_ticket_opened_at", "second_ticket_opened_at_month", "ticket_opened_at_tercile", "event_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_actors = Window.partitionBy("distinct_actors")

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("escalated", "created", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_parent_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

ranked = (
    with_parent_ticket
    .select(
        "*",
        F.round(F.percent_rank().over(w_ticket_priority_by_event_at), 3).alias("event_at_percent_rank"),
        F.nth_value("ticket_opened_at", 2).over(w_ticket_priority_ticket_id_by).alias("second_ticket_opened_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_ticket_opened_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round((F.unix_timestamp("ticket_opened_at") - F.unix_timestamp(F.lag("ticket_opened_at").over(w_actor_by_ticket_opened_at))) / 60, 1).alias("minutes_since_prev_ticket_opened_at"),
        F.first("ticket_opened_at", ignorenulls=True).over(w_ticket_priority_ticket_id_by_2).alias("first_known_ticket_opened_at"),
        F.ntile(4).over(w_actor_by_ticket_opened_at).alias("ticket_opened_at_quartile"),
        F.last("ticket_opened_at", ignorenulls=True).over(w_ticket_priority_ticket_id_by_2).alias("last_known_ticket_opened_at"),
        F.nth_value("event_at", 2).over(w_ticket_priority_by_event_at_2).alias("second_event_at"),
        F.ntile(3).over(w_actor_by_ticket_opened_at).alias("ticket_opened_at_tercile"),
        F.lag("ticket_opened_at").over(w_ticket_priority_by_event_at_3).alias("prev_ticket_opened_at"),
        F.rank().over(w_ticket_priority_by_event_at).alias("event_at_rank"),
    )
)

labeled = (
    ranked
    .select(
        "*",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.round(F.col("event_at_percent_rank") / F.nullif(F.col("event_at_rank"), F.lit(0)), 3).alias("event_at_percent_rank_per_event_at_rank"),
        F.round(F.col("event_at_percent_rank") / F.nullif(F.col("ticket_opened_at_tercile"), F.lit(0)), 3).alias("event_at_percent_rank_per_ticket_opened_at_terci"),
    )
)

per_group = (
    labeled
    .groupBy(
        F.date_trunc("month", "second_ticket_opened_at").alias("second_ticket_opened_at_month"),
        "ticket_opened_at_tercile",
        F.date_trunc("month", "event_at").alias("event_at_month"),
    )
    .agg(
        F.round(F.avg("event_at_percent_rank_per_event_at_rank"), 2).alias("avg_event_at_percent_rank_per_event_at_rank"),
        F.count("*").alias("row_count"),
        F.max("event_at_percent_rank").alias("max_event_at_percent_rank"),
        F.min("first_known_ticket_opened_at").alias("first_first_known_ticket_opened_at"),
        F.round(F.avg("minutes_since_prev_ticket_opened_at"), 2).alias("avg_minutes_since_prev_ticket_opened_at"),
        F.countDistinct("actor").alias("distinct_actors"),
    )
)

metrics = (
    per_group
    .select(
        "second_ticket_opened_at_month",
        "ticket_opened_at_tercile",
        "event_at_month",
        "avg_event_at_percent_rank_per_event_at_rank",
        "max_event_at_percent_rank",
        "first_first_known_ticket_opened_at",
        "avg_minutes_since_prev_ticket_opened_at",
        "distinct_actors",
        F.first("distinct_actors").over(w_row_count_by_avg_minutes_sin).alias("first_distinct_actors"),
        F.round(F.col("max_event_at_percent_rank") - F.avg("max_event_at_percent_rank").over(w_distinct_actors), 2).alias("max_event_at_percent_rank_vs_distinct_actors_avg"),
        F.round(F.col("max_event_at_percent_rank") / F.nullif(F.sum("max_event_at_percent_rank").over(w_distinct_actors), F.lit(0)), 4).alias("max_event_at_percent_rank_share_of_distinct_acto"),
    )
)

result = (
    metrics
    .select(
        "second_ticket_opened_at_month",
        "event_at_month",
        "avg_event_at_percent_rank_per_event_at_rank",
        "max_event_at_percent_rank",
        "first_first_known_ticket_opened_at",
        "avg_minutes_since_prev_ticket_opened_at",
        "first_distinct_actors",
        "max_event_at_percent_rank_vs_distinct_actors_avg",
        "max_event_at_percent_rank_share_of_distinct_acto",
        F.date_trunc("year", F.col("event_at_month")).alias("event_at_month_year"),
    )
    .orderBy("avg_event_at_percent_rank_per_event_at_rank")
)
