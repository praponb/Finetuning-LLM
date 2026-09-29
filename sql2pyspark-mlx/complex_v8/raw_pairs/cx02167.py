from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_all_by_ticket_closed_at = Window.orderBy(F.col("ticket_closed_at").desc(), "id")
w_all_by_ticket_closed_at_ties = Window.orderBy(F.col("ticket_closed_at").desc())
w_ticket_id_event_at_year_by_e = Window.partitionBy("ticket_id", F.year("event_at")).orderBy(F.col("event_at").desc_nulls_first(), "id")
w_actor_event_type_by_ticket_c = Window.partitionBy("actor", "event_type").orderBy(F.col("ticket_closed_at").desc(), "id")
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_type_by_ticket_c_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_ticket_events
    .filter(F.col("actor") != "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

windowed = (
    with_details
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_closed_at",
        F.round(F.percent_rank().over(w_ticket_id_by_event_at), 3).alias("event_at_percent_rank"),
        F.dense_rank().over(w_ticket_id_by_event_at).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_ticket_closed_at), 3).alias("ticket_closed_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_ticket_closed_at_ties), 3).alias("ticket_closed_at_cume_dist_2"),
        F.dense_rank().over(w_ticket_id_event_at_year_by_e).alias("event_at_dense_rank_2"),
        F.ntile(5).over(w_actor_event_type_by_ticket_c).alias("ticket_closed_at_quintile"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("last_known_event_at"),
        F.round(F.percent_rank().over(w_all_by_ticket_closed_at_ties), 3).alias("ticket_closed_at_percent_rank"),
        F.nth_value("ticket_closed_at", 2).over(w_actor_event_type_by_ticket_c_2).alias("second_ticket_closed_at"),
    )
)

result = (
    windowed
    .filter(F.col("event_at_dense_rank") < 8)
    .select(
        "id",
        "actor",
        "event_at_percent_rank",
        "event_at_dense_rank",
        "ticket_closed_at_cume_dist_2",
        "event_at_dense_rank_2",
        "ticket_closed_at_quintile",
        "ticket_closed_at_percent_rank",
    )
    .orderBy(F.col("event_at_dense_rank_2").desc_nulls_first())
)
