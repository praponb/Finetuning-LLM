from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor")
w_all_by_event_at_hour_running = Window.orderBy(F.col("event_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date_ticket_csat = Window.partitionBy(F.to_date("event_at"), "ticket_csat").orderBy(F.col("actor_len").desc(), "id")
w_all_by_event_at_hour = Window.orderBy(F.col("event_at_hour").desc(), "id")
w_actor_by_actor_len_ties = Window.partitionBy("actor").orderBy(F.col("actor_len").desc())
w_actor_by_actor_len_full = Window.partitionBy("actor").orderBy(F.col("actor_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_hour_full = Window.orderBy(F.col("event_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

enriched = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_len",
        "s.event_at_hour",
        F.col("t.csat").alias("ticket_csat"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_actor), 2).alias("ticket_csat_vs_actor_avg"),
        F.first("ticket_csat", ignorenulls=True).over(w_all_by_event_at_hour_running).alias("first_known_ticket_csat"),
        F.lead("event_at", 2).over(w_event_at_to_date_ticket_csat).alias("next2_event_at"),
        F.row_number().over(w_all_by_event_at_hour).alias("seq_no"),
        F.rank().over(w_actor_by_actor_len_ties).alias("actor_len_rank"),
        F.last("event_at").over(w_actor_by_actor_len_full).alias("last_event_at"),
        F.last("event_at").over(w_all_by_event_at_hour_full).alias("last_event_at_2"),
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        (F.col("actor_len") - F.lag("actor_len").over(w_all_by_event_at_hour)).alias("actor_len_change"),
        F.size(F.collect_set("ticket_id").over(w_actor)).alias("distinct_ticket_id_in_actor"),
    )
)

leaders = metrics.filter((F.col("seq_no") <= 5) & (F.col("distinct_ticket_id_in_actor") <= 9))

result = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_len",
        "event_at_hour",
        "ticket_csat",
        "ticket_csat_vs_actor_avg",
        "first_known_ticket_csat",
        "next2_event_at",
        "seq_no",
        "last_event_at_2",
        "rows_in_actor",
        "actor_len_change",
        "distinct_ticket_id_in_actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
    .orderBy("event_type_or_unknown")
)
