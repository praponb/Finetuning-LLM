from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_csat = Window.partitionBy("ticket_csat")
w_event_at_year_actor_by_ticke = Window.partitionBy(F.year("event_at"), "actor").orderBy("ticket_requester_id", "id")
w_ticket_closed_at_month_by_ti = Window.partitionBy(F.month("ticket_closed_at")).orderBy("ticket_csat", "id").rowsBetween(-3, -1)
w_ticket_closed_at_month_by_ti_2 = Window.partitionBy(F.month("ticket_closed_at")).orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ticket_requester_id_r = Window.orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_year_actor_by_ticke_2 = Window.partitionBy(F.year("event_at"), "actor").orderBy("ticket_requester_id")

base_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

with_trends = (
    joined
    .select(
        "id",
        "ticket_id",
        "ticket_csat",
        "ticket_closed_at",
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_csat), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_csat"),
        F.ntile(5).over(w_event_at_year_actor_by_ticke).alias("ticket_requester_id_quintile"),
        F.round(F.col("ticket_requester_id") / F.nullif(F.sum("ticket_requester_id").over(w_ticket_csat), F.lit(0)), 4).alias("ticket_requester_id_share_of_ticket_csat"),
        F.size(F.collect_set("ticket_id").over(w_ticket_csat)).alias("distinct_ticket_id_in_ticket_csat"),
        F.max("ticket_csat").over(w_ticket_closed_at_month_by_ti).alias("ticket_csat_max_3_prior"),
        F.last("ticket_requester_id", ignorenulls=True).over(w_ticket_closed_at_month_by_ti_2).alias("last_known_ticket_requester_id"),
        F.first("ticket_closed_at", ignorenulls=True).over(w_ticket_closed_at_month_by_ti_2).alias("first_known_ticket_closed_at"),
        F.max("ticket_requester_id").over(w_all_by_ticket_requester_id_r).alias("max_ticket_requester_id_so_far"),
        F.first("ticket_requester_id", ignorenulls=True).over(w_ticket_closed_at_month_by_ti_2).alias("first_known_ticket_requester_id"),
        F.round(F.cume_dist().over(w_event_at_year_actor_by_ticke_2), 3).alias("ticket_requester_id_cume_dist"),
        F.count(F.lit(1)).over(w_ticket_csat).alias("rows_in_ticket_csat"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_ticket_csat), 2).alias("ticket_requester_id_vs_ticket_csat_avg"),
    )
)

derived = (
    with_trends
    .withColumn("ticket_csat_max_3_prior_filled", F.coalesce(F.col("ticket_csat_max_3_prior"), F.lit(0)))
)

result = (
    derived
    .select(
        "id",
        "ticket_id",
        "ticket_csat",
        "ticket_csat_share_of_ticket_csat",
        "ticket_requester_id_quintile",
        "ticket_requester_id_share_of_ticket_csat",
        "distinct_ticket_id_in_ticket_csat",
        "last_known_ticket_requester_id",
        "first_known_ticket_closed_at",
        "max_ticket_requester_id_so_far",
        "ticket_requester_id_cume_dist",
        "rows_in_ticket_csat",
        "ticket_requester_id_vs_ticket_csat_avg",
        "ticket_csat_max_3_prior_filled",
    )
    .orderBy(F.col("first_known_ticket_closed_at").desc())
)
