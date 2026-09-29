from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_last_opened_at_ye = Window.partitionBy("event_type", F.year("last_opened_at"))
w_all_by_pct_event_actor_custo = Window.orderBy(F.col("pct_event_actor_customer").desc(), "event_type", "event_actor").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
    )
)

detail_rows_events = (
    recent_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.tags",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

grouped = (
    detail_rows_events
    .groupBy("event_type", "event_actor")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.round(100 * F.count(F.when(F.col("event_actor") == "customer", 1)) / F.count("*"), 1).alias("pct_event_actor_customer"),
        F.max("opened_at").alias("last_opened_at"),
        F.count("*").alias("cnt"),
    )
)

without_matches = (
    grouped
    .crossJoin(grouped.agg(F.max("pct_event_actor_customer").alias("overall_max_pct_event_actor_customer")))
    .filter(F.col("pct_event_actor_customer") < F.col("overall_max_pct_event_actor_customer"))
    .drop("overall_max_pct_event_actor_customer")
)

screened = (
    without_matches
    .crossJoin(without_matches.agg(F.min("cnt").alias("overall_min_cnt")))
    .filter(F.col("cnt") > F.col("overall_min_cnt"))
    .drop("overall_min_cnt")
)

ranked = (
    screened
    .select(
        "*",
        F.count(F.lit(1)).over(w_event_type_last_opened_at_ye).alias("rows_in_event_type_last_opened_at_year"),
        F.round(F.col("cnt") - F.avg("cnt").over(w_event_type_last_opened_at_ye), 2).alias("cnt_vs_event_type_last_opened_at_year_avg"),
        F.min("pct_event_actor_customer").over(w_all_by_pct_event_actor_custo).alias("min_pct_event_actor_customer_so_far"),
        F.round(F.avg("cnt").over(w_event_type_last_opened_at_ye), 2).alias("event_type_last_opened_at_year_avg_cnt"),
    )
)

result = (
    ranked
    .select(
        "event_type",
        "pct_event_actor_customer",
        "last_opened_at",
        "rows_in_event_type_last_opened_at_year",
        "cnt_vs_event_type_last_opened_at_year_avg",
        "min_pct_event_actor_customer_so_far",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
    .orderBy(
        "cnt_vs_event_type_last_opened_at_year_avg",
        F.col("cnt_filled").asc_nulls_last(),
    )
)
