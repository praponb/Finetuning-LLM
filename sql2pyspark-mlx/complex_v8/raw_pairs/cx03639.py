from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_all_by_days_since_event_at_t = Window.orderBy(F.col("days_since_event_at").asc_nulls_last())
w_ticket_csat_by_ticket_opened = Window.partitionBy("ticket_csat").orderBy("ticket_opened_at", "id").rowsBetween(-6, Window.currentRow)
w_ticket_csat_by_ticket_opened_2 = Window.partitionBy("ticket_csat").orderBy("ticket_opened_at")

base = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_type") != "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_activity = (
    base
    .crossJoin(base.agg(F.avg("days_since_event_at").alias("overall_avg_days_since_event_at")))
    .filter(F.col("days_since_event_at") > F.col("overall_avg_days_since_event_at"))
    .drop("overall_avg_days_since_event_at")
)

metrics = (
    with_activity
    .select(
        "*",
        F.lag("days_since_event_at", 1, 0).over(w_event_type_ticket_id_by_even).alias("prev_days_since_event_at"),
        F.rank().over(w_all_by_days_since_event_at_t).alias("days_since_event_at_rank"),
        F.dense_rank().over(w_all_by_days_since_event_at_t).alias("days_since_event_at_dense_rank"),
    )
)

with_parent_ticket = (
    metrics.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat").between(2, 4)), "left")
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

windowed = (
    with_parent_ticket
    .select(
        "*",
        F.round(F.avg("days_since_event_at").over(w_ticket_csat_by_ticket_opened), 2).alias("days_since_event_at_avg_7"),
        F.dense_rank().over(w_ticket_csat_by_ticket_opened_2).alias("ticket_opened_at_dense_rank"),
    )
)

leaders = (
    windowed
    .filter(F.col("ticket_opened_at_dense_rank") == 1)
    .drop("ticket_opened_at_dense_rank")
)

result = (
    leaders
    .select(
        "id",
        "days_since_event_at",
        "days_since_event_at_rank",
        "ticket_csat",
    )
    .unpivot(
        ids=["id"],
        values=["days_since_event_at", "days_since_event_at_rank", "ticket_csat"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
