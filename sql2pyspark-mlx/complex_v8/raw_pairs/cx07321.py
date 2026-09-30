from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_by_ticket_csat = Window.partitionBy("actor").orderBy("ticket_csat", "id")
w_actor_by_ticket_csat_running = Window.partitionBy("actor").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_ticket_csat_moving = Window.partitionBy("actor").orderBy("ticket_csat", "id").rowsBetween(-1, Window.currentRow)
w_event_at_to_date = Window.partitionBy(F.to_date("event_at"))
w_actor_by_ticket_csat_running_2 = Window.partitionBy("actor").orderBy(F.col("ticket_csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.size(F.collect_set("actor").over(w_ticket_id)).alias("distinct_actor_in_ticket_id"),
        F.round(F.percent_rank().over(w_all_by_event_at), 3).alias("event_at_percent_rank"),
        F.round(F.percent_rank().over(w_actor_by_ticket_csat), 3).alias("ticket_csat_percent_rank"),
        F.sum("ticket_csat").over(w_actor_by_ticket_csat_running).alias("running_ticket_csat"),
        F.round(F.avg("ticket_csat").over(w_actor_by_ticket_csat_moving), 2).alias("ticket_csat_avg_2"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_event_at_to_date), F.lit(0)), 4).alias("ticket_csat_share_of_event_at_to_date"),
        F.sum("ticket_csat").over(w_actor_by_ticket_csat_running_2).alias("running_ticket_csat_2"),
        F.lag("ticket_csat", 3).over(w_actor_by_ticket_csat).alias("prev3_ticket_csat"),
    )
)

labeled = (
    sequenced
    .filter(F.col("ticket_csat_share_of_event_at_to_date").between(0.3, 0.8))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_opened_at",
        "ticket_csat",
        "distinct_actor_in_ticket_id",
        "event_at_percent_rank",
        "ticket_csat_avg_2",
        "ticket_csat_share_of_event_at_to_date",
        "running_ticket_csat_2",
        "prev3_ticket_csat",
        F.greatest(F.col("event_at_percent_rank"), F.col("ticket_csat_percent_rank")).alias("greatest_event_at_percent_rank_ticket_csat_perce"),
    )
)

stats = (
    labeled
    .groupBy("ticket_id", "actor")
    .agg(
        F.min("ticket_opened_at").alias("first_ticket_opened_at"),
        F.round(F.sum("greatest_event_at_percent_rank_ticket_csat_perce") / F.nullif(F.sum("prev3_ticket_csat"), F.lit(0)), 3).alias("greatest_event_at_percent_rank_ticket_csat_perce_2"),
        F.round(F.sum("running_ticket_csat_2") / F.nullif(F.sum("prev3_ticket_csat"), F.lit(0)), 3).alias("running_ticket_csat_2_to_prev3_ticket_csat_ratio"),
        F.sum("ticket_csat_avg_2").alias("total_ticket_csat_avg_2"),
        F.percentile_approx("ticket_csat_avg_2", 0.5).alias("median_ticket_csat_avg_2"),
        F.max("greatest_event_at_percent_rank_ticket_csat_perce").alias("max_greatest_event_at_percent_rank_ticket_csat_p"),
    )
    .filter(F.col("max_greatest_event_at_percent_rank_ticket_csat_p") > 0.3)
    .drop("max_greatest_event_at_percent_rank_ticket_csat_p")
)

result = (
    stats
    .select(
        "ticket_id",
        "actor",
        "first_ticket_opened_at",
        "greatest_event_at_percent_rank_ticket_csat_perce_2",
        "running_ticket_csat_2_to_prev3_ticket_csat_ratio",
        "total_ticket_csat_avg_2",
        "median_ticket_csat_avg_2",
        F.abs(F.col("median_ticket_csat_avg_2") - 3.0).alias("median_ticket_csat_avg_2_distance"),
    )
)
