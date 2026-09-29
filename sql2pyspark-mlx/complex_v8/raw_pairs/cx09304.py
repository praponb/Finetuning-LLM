from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_next_event_at_month_seq_no = Window.partitionBy(F.date_trunc("month", "next_event_at"), "seq_no")
w_all_by_seq_no = Window.orderBy("seq_no", "id")

recent_events = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-18").cast("date")) & (F.col("event_type") != "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

latest = (
    recent_events
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    latest
    .select(
        "*",
        F.lead("event_at").over(w_event_type_actor_by_event_at).alias("next_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.row_number().over(w_ticket_id_by_event_at).alias("seq_no"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("event_at_cume_dist").over(w_next_event_at_month_seq_no).alias("next_event_at_month_seq_no_sum_event_at_cume_dis"),
        F.lead("seq_no").over(w_all_by_seq_no).alias("next_seq_no"),
        F.round(F.percent_rank().over(w_all_by_seq_no), 3).alias("seq_no_percent_rank"),
    )
)

result = (
    with_windows
    .cube("next_seq_no", "ticket_id", "seq_no")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("actor") != "customer", 1)).alias("n_actor_customer"),
    )
)
