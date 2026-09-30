from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_event_type_by = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_at_month_event_type_by_2 = Window.partitionBy(F.date_trunc("month", "event_at"), "event_type").orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_ticket_id_actor_by_event_at_3 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_ticket_id_by = Window.partitionBy(F.date_trunc("month", "event_at"), "ticket_id").orderBy(F.col("event_at").desc())
w_event_at_quintile_next_event = Window.partitionBy("event_at_quintile", F.month("next_event_at"))
w_first_event_at_month_by_even = Window.partitionBy(F.date_trunc("month", "first_event_at")).orderBy("event_at_rank_2", "id")
w_all_by_first_event_at = Window.orderBy("first_event_at", "id")

filtered_events = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    filtered_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.first("event_at").over(w_event_at_month_event_type_by).alias("first_event_at"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at_2"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.rank().over(w_ticket_id_actor_by_event_at).alias("event_at_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.first("event_at", ignorenulls=True).over(w_ticket_id_actor_by_event_at_2).alias("first_known_event_at"),
        F.ntile(10).over(w_ticket_id_by_event_at).alias("event_at_decile"),
        F.dense_rank().over(w_ticket_id_by_event_at).alias("event_at_dense_rank"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank_2"),
        F.lead("event_at", 3).over(w_event_at_month_event_type_by_2).alias("next3_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_event_at_month_event_type_by_2).alias("next_event_at"),
        F.last("event_at").over(w_ticket_id_actor_by_event_at_3).alias("last_event_at"),
        F.round(F.percent_rank().over(w_event_at_month_ticket_id_by), 3).alias("event_at_percent_rank_2"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "first_event_at",
        "first_event_at_2",
        "event_at_cume_dist",
        "event_at_quintile",
        "event_at_rank",
        "minutes_since_prev_event_at",
        "first_known_event_at",
        "event_at_decile",
        "event_at_rank_2",
        "event_at_percent_rank",
        "next_event_at",
        "last_event_at",
        "event_at_percent_rank_2",
        F.round(F.avg("minutes_since_prev_event_at").over(w_event_at_quintile_next_event), 2).alias("event_at_quintile_next_event_at_month_avg_minute"),
        F.ntile(3).over(w_first_event_at_month_by_even).alias("event_at_rank_2_tercile"),
        F.ntile(3).over(w_all_by_first_event_at).alias("first_event_at_tercile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "event_type",
        "actor",
        "first_event_at",
        "first_event_at_2",
        "event_at_cume_dist",
        "event_at_quintile",
        "event_at_rank",
        "minutes_since_prev_event_at",
        "first_known_event_at",
        "event_at_decile",
        "event_at_rank_2",
        "event_at_percent_rank",
        "event_at_percent_rank_2",
        "first_event_at_tercile",
        F.least(F.col("event_at_decile"), F.col("event_at_cume_dist")).alias("least_event_at_decile_event_at_cume_dist"),
        F.round(F.col("first_event_at_tercile") / F.nullif(F.col("event_at_cume_dist"), F.lit(0)), 3).alias("first_event_at_tercile_per_event_at_cume_dist"),
    )
    .orderBy(F.col("first_event_at_tercile").asc_nulls_last(), F.col("actor").desc())
)
