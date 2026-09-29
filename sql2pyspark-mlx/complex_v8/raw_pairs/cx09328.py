from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_actor = Window.partitionBy("actor")
w_ticket_id_actor_by_days_sinc = Window.partitionBy("ticket_id", "actor").orderBy(F.col("days_since_event_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_ticket_id_event_type_by_days = Window.partitionBy("ticket_id", "event_type").orderBy("days_since_event_at")
w_all_by_event_at_moving_6 = Window.orderBy("event_at", "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_event_type_by_days_2 = Window.partitionBy("ticket_id", "event_type").orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .filter((F.col("actor") == "agent") & (F.col("event_type").isin("resolved", "reopened")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "days_since_event_at",
        F.sum("days_since_event_at").over(w_ticket_id_event_type).alias("ticket_id_event_type_sum_days_since_event_at"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_actor), F.lit(0)), 4).alias("days_since_event_at_share_of_actor"),
        F.max("days_since_event_at").over(w_ticket_id_actor_by_days_sinc).alias("days_since_event_at_max_3"),
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_ticket_id_event_type), F.lit(0)), 4).alias("days_since_event_at_share_of_ticket_id_event_typ"),
        F.max("days_since_event_at").over(w_actor).alias("actor_max_days_since_event_at"),
        F.round(F.avg("days_since_event_at").over(w_ticket_id_event_type), 2).alias("ticket_id_event_type_avg_days_since_event_at"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "days_since_event_at",
        "ticket_id_event_type_sum_days_since_event_at",
        "days_since_event_at_share_of_actor",
        "rows_in_actor",
        "days_since_event_at_share_of_ticket_id_event_typ",
        F.max("days_since_event_at").over(w_ticket_id_event_type).alias("ticket_id_event_type_max_days_since_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.round(F.cume_dist().over(w_ticket_id_event_type_by_days), 3).alias("days_since_event_at_cume_dist"),
        F.round(F.col("rows_in_actor") - F.avg("rows_in_actor").over(w_ticket_id_event_type), 2).alias("rows_in_actor_vs_ticket_id_event_type_avg"),
        F.count(F.lit(1)).over(w_ticket_id_event_type).alias("rows_in_ticket_id_event_type"),
        F.sum("rows_in_actor").over(w_all_by_event_at_moving_6).alias("rows_in_actor_sum_7"),
        F.sum("days_since_event_at").over(w_ticket_id_event_type_by_days_2).alias("running_days_since_event_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "days_since_event_at",
        "ticket_id_event_type_sum_days_since_event_at",
        "days_since_event_at_share_of_actor",
        "rows_in_actor",
        "days_since_event_at_share_of_ticket_id_event_typ",
        "ticket_id_event_type_max_days_since_event_at",
        "rows_in_actor_vs_ticket_id_event_type_avg",
        "running_days_since_event_at",
        (F.col("days_since_event_at") + F.col("event_at_percent_rank")).alias("days_since_event_at_plus_event_at_percent_rank"),
    )
    .orderBy(F.col("ticket_id").desc())
)
