from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_group_by_event_at_runn = Window.partitionBy("actor_group").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_group_by_event_at = Window.partitionBy("actor_group").orderBy("event_at", "id")
w_event_at_month_actor_by_even = Window.partitionBy(F.date_trunc("month", "event_at"), "actor").orderBy("event_at", "id")
w_event_at_month_actor_by_even_2 = Window.partitionBy(F.date_trunc("month", "event_at"), "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_group_by_event_at_full = Window.partitionBy("actor_group").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_group_by_event_at_ties = Window.partitionBy("actor_group").orderBy("event_at")

base = (
    df_ticket_events
    .filter(F.col("actor") != "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
    )
)

metrics = (
    base
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor_group",
        F.last("event_at", ignorenulls=True).over(w_actor_group_by_event_at_runn).alias("last_known_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_group_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_actor_by_even))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.last("event_at", ignorenulls=True).over(w_event_at_month_actor_by_even_2).alias("last_known_event_at_2"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.nth_value("event_at", 2).over(w_actor_group_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_actor_group_by_event_at_ties).alias("event_at_rank_2"),
        F.ntile(5).over(w_actor_group_by_event_at).alias("event_at_quintile"),
    )
)

result = (
    metrics
    .filter(F.col("last_known_event_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor_group",
        "last_known_event_at",
        "minutes_since_prev_event_at",
        "minutes_since_prev_event_at_2",
        "last_known_event_at_2",
        "seq_no",
        "event_at_rank",
        "event_at_rank_2",
        "event_at_quintile",
        (F.floor(F.col("event_at_quintile") / 2) * 2).alias("event_at_quintile_bucket"),
        F.abs(F.col("seq_no") - 4).alias("seq_no_distance"),
    )
    .orderBy(F.col("ticket_id").asc(), F.col("last_known_event_at").asc())
)
