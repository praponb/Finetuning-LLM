from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc())
w_event_type = Window.partitionBy("event_type")
w_actor_next2_event_at_month_b = Window.partitionBy("actor", F.date_trunc("month", "next2_event_at")).orderBy(F.col("event_at_percent_rank_2").desc_nulls_first())
w_actor_next2_event_at_month_b_2 = Window.partitionBy("actor", F.date_trunc("month", "next2_event_at")).orderBy(F.col("event_at_percent_rank_2").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    scoped_events
    .select(
        "*",
        F.round(F.percent_rank().over(w_ticket_id_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.ntile(3).over(w_ticket_id_actor_by_event_at).alias("event_at_tercile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_actor_by_event_at_2), 3).alias("event_at_percent_rank_2"),
        F.lead("event_at", 2).over(w_all_by_event_at).alias("next2_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_stats = (
    sequenced.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.first_event_at_2")
)

sequenced_2 = (
    with_stats
    .select(
        "*",
        F.round(F.col("event_at_cume_dist") - F.avg("event_at_cume_dist").over(w_event_type), 2).alias("event_at_cume_dist_vs_event_type_avg"),
        F.round(F.cume_dist().over(w_actor_next2_event_at_month_b), 3).alias("event_at_percent_rank_2_cume_dist"),
        F.first("event_at", ignorenulls=True).over(w_actor_next2_event_at_month_b_2).alias("first_known_event_at"),
        F.last("minutes_since_prev_event_at", ignorenulls=True).over(w_actor_next2_event_at_month_b_2).alias("last_known_minutes_since_prev_event_at"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
    )
)

joined = (
    sequenced_2.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_percent_rank",
        "s.event_at_tercile",
        "s.minutes_since_prev_event_at",
        "s.event_at_percent_rank_2",
        "s.next2_event_at",
        "s.event_at_cume_dist",
        "s.first_event_at",
        "s.first_event_at_2",
        "s.event_at_cume_dist_vs_event_type_avg",
        "s.event_at_percent_rank_2_cume_dist",
        "s.first_known_event_at",
        "s.rows_in_event_type",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    joined
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_percent_rank",
        "event_at_tercile",
        "minutes_since_prev_event_at",
        "event_at_percent_rank_2",
        "next2_event_at",
        "event_at_cume_dist",
        "first_event_at",
        "event_at_cume_dist_vs_event_type_avg",
        "event_at_percent_rank_2_cume_dist",
        "first_known_event_at",
        "rows_in_event_type",
        "ticket_opened_at",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        (F.col("event_at_cume_dist") + F.col("rows_in_event_type")).alias("event_at_cume_dist_plus_rows_in_event_type"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("first_event_at").asc())
    .limit(20)
)
