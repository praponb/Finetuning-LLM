from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_event_at_to_date = Window.partitionBy("event_type", F.to_date("event_at"))
w_all_by_last_event_at = Window.orderBy(F.col("last_event_at").desc(), "id")
w_all_by_last_event_at_full = Window.orderBy(F.col("last_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_event_at_t = Window.orderBy(F.col("days_since_event_at").desc())
w_prev2_event_at_month_ticket = Window.partitionBy(F.month("prev2_event_at"), "ticket_id")
w_all_by_days_since_event_at = Window.orderBy(F.col("days_since_event_at").desc(), "id")

scoped_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

metrics = (
    summarized_rows
    .select(
        "id",
        "ticket_id",
        "actor_len",
        "days_since_event_at",
        "event_type_actor_label",
        F.round(F.col("actor_len") / F.nullif(F.sum("actor_len").over(w_event_type_event_at_to_date), F.lit(0)), 4).alias("actor_len_share_of_event_type_event_at_to_date"),
        F.dense_rank().over(w_all_by_last_event_at).alias("last_event_at_dense_rank"),
        F.nth_value("days_since_event_at", 2).over(w_all_by_last_event_at_full).alias("second_days_since_event_at"),
        F.lag("event_at", 2).over(w_all_by_last_event_at).alias("prev2_event_at"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_days_since_event_at_t), 3).alias("days_since_event_at_cume_dist"),
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_prev2_event_at_month_ticket), 2).alias("days_since_event_at_vs_prev2_event_at_month_tick"),
        F.rank().over(w_all_by_days_since_event_at).alias("days_since_event_at_rank"),
        (F.col("last_event_at_dense_rank") - F.lag("last_event_at_dense_rank").over(w_all_by_days_since_event_at)).alias("last_event_at_dense_rank_change"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["last_event_at_dense_rank", "actor_len"], ["last_event_at_dense_rank"], ["actor_len"], []],
        "last_event_at_dense_rank", "actor_len",
    )
    .agg(
        F.max("prev2_event_at").alias("last_prev2_event_at"),
        F.min("prev2_event_at").alias("first_prev2_event_at"),
    )
)
