from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_seq_no_running = Window.partitionBy("ticket_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_seq_no_moving_2 = Window.partitionBy("ticket_id").orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)
w_dedup = Window.partitionBy("event_type").orderBy(F.col("ticket_opened_at").desc(), F.col("id").desc())

recent = (
    df_ticket_events
    .filter(~F.col("actor").isin("agent", "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "replied"), "group_a").when(F.col("event_type") == "created", "group_b").otherwise("other").alias("event_type_group"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

sequenced = (
    recent
    .withColumn("seq_no", F.row_number().over(w_ticket_id_actor_by_event_at))
    .withColumn("next_event_at_month", F.lead("event_at_month").over(w_all_by_event_at))
)

ranked = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_at_label",
        "event_at_month",
        "seq_no",
        "next_event_at_month",
        F.first("event_at", ignorenulls=True).over(w_ticket_id_by_seq_no_running).alias("first_known_event_at"),
        F.round(F.avg("seq_no").over(w_ticket_id_by_seq_no_moving_2), 2).alias("seq_no_avg_3"),
    )
)

with_parent = (
    ranked.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_label",
        "s.next_event_at_month",
        "s.seq_no_avg_3",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

unique_rows = (
    with_parent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "event_at_label",
        "next_event_at_month",
        "seq_no_avg_3",
        "ticket_opened_at",
        "ticket_tags",
        F.date_format(F.col("next_event_at_month"), "yyyy-MM-dd").alias("next_event_at_month_label"),
    )
)
