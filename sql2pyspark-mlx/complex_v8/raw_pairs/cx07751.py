from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_ticket_id_by = Window.partitionBy(F.date_trunc("month", "event_at"), "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-18").cast("date")) & (~F.col("actor").isin("system", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_ticket_id_actor_by_event_at).alias("second_event_at"),
        F.round(F.cume_dist().over(w_event_at_month_ticket_id_by), 3).alias("event_at_cume_dist"),
        F.ntile(4).over(w_ticket_id_actor_by_event_at_2).alias("event_at_quartile"),
        F.row_number().over(w_event_at_month_ticket_id_by).alias("seq_no"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_ties), 3).alias("event_at_cume_dist_2"),
        F.dense_rank().over(w_event_type_by_event_at).alias("event_at_dense_rank"),
        F.lag("event_at", 2).over(w_ticket_id_actor_by_event_at_2).alias("prev2_event_at"),
        F.lag("event_at").over(w_ticket_id_actor_by_event_at_2).alias("prev_event_at"),
        F.dense_rank().over(w_actor_by_event_at_ties).alias("event_at_dense_rank_2"),
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("last_known_event_at"),
        F.round(F.cume_dist().over(w_event_type_by_event_at), 3).alias("event_at_cume_dist_3"),
        F.row_number().over(w_ticket_id_actor_by_event_at_2).alias("event_at_seq"),
        F.round(F.percent_rank().over(w_event_type_by_event_at), 3).alias("event_at_percent_rank"),
        F.rank().over(w_event_type_by_event_at).alias("event_at_rank"),
        F.first("event_at").over(w_ticket_id_actor_by_event_at).alias("first_event_at"),
    )
)

scored = (
    metrics
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "second_event_at",
        "event_at_cume_dist",
        "event_at_quartile",
        "seq_no",
        "event_at_cume_dist_2",
        "event_at_dense_rank_2",
        "last_known_event_at",
        "event_at_cume_dist_3",
        "event_at_seq",
        "event_at_percent_rank",
        F.least(F.col("event_at_cume_dist_3"), F.col("event_at_cume_dist")).alias("least_event_at_cume_dist_3_event_at_cume_dist"),
        F.round(F.col("event_at_cume_dist_3") * 1.1, 2).alias("event_at_cume_dist_3_adj"),
    )
)

result = (
    scored
    .filter(F.col("event_at_percent_rank") > 0.2)
    .select(
        "id",
        "event_type",
        "actor",
        "second_event_at",
        "event_at_quartile",
        "seq_no",
        "event_at_cume_dist_2",
        "event_at_dense_rank_2",
        "last_known_event_at",
        "event_at_cume_dist_3",
        "event_at_seq",
        "event_at_percent_rank",
    )
    .orderBy(
        F.col("event_at_cume_dist_2").asc_nulls_last(),
        F.col("event_at_seq").desc(),
    )
)
