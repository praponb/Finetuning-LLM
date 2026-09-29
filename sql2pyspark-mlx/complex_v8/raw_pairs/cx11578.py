from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_at_month = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at"))

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("created", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

with_windows = (
    filtered_events
    .select(
        "id",
        "event_at",
        "actor",
        "actor_len",
        "event_at_plus_15m",
        F.round(F.col("actor_len") - F.avg("actor_len").over(w_ticket_id_event_at_month), 2).alias("actor_len_vs_ticket_id_event_at_month_avg"),
        F.round(F.col("actor_len") / F.nullif(F.sum("actor_len").over(w_ticket_id_event_at_month), F.lit(0)), 4).alias("actor_len_share_of_ticket_id_event_at_month"),
        F.count(F.lit(1)).over(w_ticket_id_event_at_month).alias("rows_in_ticket_id_event_at_month"),
    )
)

with_activity = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("rows_in_ticket_id_event_at_month").alias("overall_avg_rows_in_ticket_id_event_at_month")))
    .filter(F.col("rows_in_ticket_id_event_at_month") > F.col("overall_avg_rows_in_ticket_id_event_at_month"))
    .drop("overall_avg_rows_in_ticket_id_event_at_month")
)

qualified = (
    with_activity
    .crossJoin(with_activity.agg(F.max("actor_len").alias("overall_max_actor_len")))
    .filter(F.col("actor_len") < F.col("overall_max_actor_len"))
    .drop("overall_max_actor_len")
)

result = (
    qualified
    .select(
        "id",
        "event_at",
        "actor",
        "event_at_plus_15m",
        "actor_len_vs_ticket_id_event_at_month_avg",
        "actor_len_share_of_ticket_id_event_at_month",
        "rows_in_ticket_id_event_at_month",
    )
    .orderBy(F.col("id").desc())
)
