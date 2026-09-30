from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_at_month = Window.partitionBy("ticket_id", F.month("event_at"))
w_ticket_priority_actor = Window.partitionBy("ticket_priority", "actor")
w_ticket_id_by_ticket_csat = Window.partitionBy("ticket_id").orderBy("ticket_csat", "id")
w_ticket_id_by_ticket_csat_run = Window.partitionBy("ticket_id").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_ticket_csat_by_ticket = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("ticket_csat").desc(), "id")
w_actor_by_ticket_csat_ties = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc())
w_actor_by_ticket_csat = Window.partitionBy("actor").orderBy(F.col("ticket_csat").desc(), "id")

filtered = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 19)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.csat") >= 2)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.sum("ticket_csat").over(w_ticket_id_event_at_month).alias("ticket_id_event_at_month_sum_ticket_csat"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_priority_actor), 2).alias("ticket_csat_vs_ticket_priority_actor_avg"),
        F.max("ticket_csat").over(w_ticket_id_event_at_month).alias("ticket_id_event_at_month_max_ticket_csat"),
        F.round(F.percent_rank().over(w_ticket_id_by_ticket_csat), 3).alias("ticket_csat_percent_rank"),
        F.last("ticket_csat", ignorenulls=True).over(w_ticket_id_by_ticket_csat_run).alias("last_known_ticket_csat"),
        F.count(F.lit(1)).over(w_ticket_id_event_at_month).alias("rows_in_ticket_id_event_at_month"),
        F.sum("ticket_csat").over(w_ticket_priority_actor).alias("ticket_priority_actor_sum_ticket_csat"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.ntile(5).over(w_actor_ticket_csat_by_ticket).alias("ticket_csat_quintile"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_priority_actor), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_priority_actor"),
        F.min("ticket_csat").over(w_ticket_id_event_at_month).alias("ticket_id_event_at_month_min_ticket_csat"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_id_event_at_month), 2).alias("ticket_csat_vs_ticket_id_event_at_month_avg"),
        F.round(F.cume_dist().over(w_actor_by_ticket_csat_ties), 3).alias("ticket_csat_cume_dist"),
        F.size(F.collect_set("ticket_id").over(w_ticket_priority_actor)).alias("distinct_ticket_id_in_ticket_priority_actor"),
        F.dense_rank().over(w_actor_by_ticket_csat).alias("ticket_csat_dense_rank"),
    )
)

first_rows = (
    ranked
    .filter((F.col("ticket_csat_dense_rank") <= 3) & (F.col("ticket_csat_percent_rank") > 0.4))
)

result = (
    first_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_csat",
        "ticket_priority",
        "ticket_id_event_at_month_sum_ticket_csat",
        "ticket_csat_vs_ticket_priority_actor_avg",
        "ticket_id_event_at_month_max_ticket_csat",
        "last_known_ticket_csat",
        "rows_in_ticket_id_event_at_month",
        "ticket_priority_actor_sum_ticket_csat",
        "event_at_cume_dist",
        "ticket_csat_share_of_ticket_priority_actor",
        "ticket_id_event_at_month_min_ticket_csat",
        "ticket_csat_cume_dist",
        "ticket_csat_dense_rank",
    )
    .orderBy(F.col("ticket_csat_cume_dist").desc(), F.col("ticket_csat").asc())
)
