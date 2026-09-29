from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type = Window.partitionBy("actor", "event_type")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_event_at_month = Window.partitionBy("event_type", F.date_trunc("month", "event_at"))
w_event_at_year_actor_by_days = Window.partitionBy(F.year("event_at"), "actor").orderBy("days_since_event_at")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_event_at_year_actor_by_days_2 = Window.partitionBy(F.year("event_at"), "actor").orderBy("days_since_event_at", "id")
w_event_type_ticket_id_by_days = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("days_since_event_at_percent_rank").desc(), "id").rowsBetween(-1, Window.currentRow)
w_ticket_id_distinct_ticket_id = Window.partitionBy("ticket_id", "distinct_ticket_id_in_actor_event_type")
w_actor_by_days_since_event_at = Window.partitionBy("actor").orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(1, 2))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_windows = (
    recent_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "days_since_event_at",
        F.count(F.lit(1)).over(w_actor_event_type).alias("rows_in_actor_event_type"),
        F.sum("days_since_event_at").over(w_event_type_by_event_at_runni).alias("running_days_since_event_at"),
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_event_type_event_at_month), 2).alias("days_since_event_at_vs_event_type_event_at_month"),
        F.round(F.percent_rank().over(w_event_at_year_actor_by_days), 3).alias("days_since_event_at_percent_rank"),
        F.round(F.cume_dist().over(w_event_type_by_event_at), 3).alias("event_at_cume_dist"),
        F.size(F.collect_set("ticket_id").over(w_actor_event_type)).alias("distinct_ticket_id_in_actor_event_type"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.row_number().over(w_event_at_year_actor_by_days_2).alias("days_since_event_at_seq"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_actor_event_type), F.lit(0)), 4).alias("days_since_event_at_share_of_actor_event_type"),
    )
)

first_rows = with_windows.filter(F.col("event_at_dense_rank") <= 2)

with_windows_2 = (
    first_rows
    .select(
        "id",
        "event_type",
        "actor",
        "days_since_event_at",
        "rows_in_actor_event_type",
        "days_since_event_at_vs_event_type_event_at_month",
        "days_since_event_at_percent_rank",
        "event_at_cume_dist",
        "distinct_ticket_id_in_actor_event_type",
        "event_at_dense_rank",
        F.sum("days_since_event_at_seq").over(w_event_type_ticket_id_by_days).alias("days_since_event_at_seq_sum_2"),
        F.sum("event_at_dense_rank").over(w_ticket_id_distinct_ticket_id).alias("ticket_id_distinct_ticket_id_in_actor_event_type"),
        F.last("event_at_cume_dist", ignorenulls=True).over(w_actor_by_days_since_event_at).alias("last_known_event_at_cume_dist"),
    )
)

result = (
    with_windows_2
    .filter(F.col("ticket_id_distinct_ticket_id_in_actor_event_type") >= 31)
    .select(
        "id",
        "event_type",
        "days_since_event_at",
        "days_since_event_at_vs_event_type_event_at_month",
        "days_since_event_at_percent_rank",
        "days_since_event_at_seq_sum_2",
        "ticket_id_distinct_ticket_id_in_actor_event_type",
        "last_known_event_at_cume_dist",
    )
    .orderBy(
        F.col("days_since_event_at").desc(),
        "ticket_id_distinct_ticket_id_in_actor_event_type",
    )
)
