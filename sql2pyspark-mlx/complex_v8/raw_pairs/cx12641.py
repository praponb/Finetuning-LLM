from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_ticket_op = Window.partitionBy("ticket_id", "actor").orderBy("ticket_opened_at", "id")
w_ticket_opened_at_to_date_by = Window.partitionBy(F.to_date("ticket_opened_at")).orderBy("ticket_opened_at", "id")
w_ticket_id_actor_by_ticket_op_2 = Window.partitionBy("ticket_id", "actor").orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ticket_opened_at_ties = Window.orderBy(F.col("ticket_opened_at").desc())
w_dedup = Window.partitionBy("event_type", "actor").orderBy("ticket_opened_at_decile", "id")

valid = (
    df_ticket_events
    .filter(F.col("actor") != "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_details = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "ticket_opened_at",
        F.ntile(10).over(w_ticket_id_actor_by_ticket_op).alias("ticket_opened_at_decile"),
        F.row_number().over(w_ticket_opened_at_to_date_by).alias("ticket_opened_at_seq"),
        F.last("event_at", ignorenulls=True).over(w_ticket_id_actor_by_ticket_op_2).alias("last_known_event_at"),
        F.round(F.percent_rank().over(w_all_by_ticket_opened_at_ties), 3).alias("ticket_opened_at_percent_rank"),
        F.lag("ticket_opened_at", 3).over(w_ticket_id_actor_by_ticket_op).alias("prev3_ticket_opened_at"),
    )
)

event_type_stats = (
    with_trends
    .groupBy("event_type")
    .agg(F.avg("ticket_opened_at_seq").alias("avg_ticket_opened_at_seq"))
)

vs_group = (
    with_trends.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.ticket_opened_at_seq") > F.col("g.avg_ticket_opened_at_seq"))
    .select(
        "s.*",
        "g.avg_ticket_opened_at_seq",
        F.round(F.col("s.ticket_opened_at_seq") - F.col("g.avg_ticket_opened_at_seq"), 2).alias("ticket_opened_at_seq_minus_avg"),
    )
)

deduped = (
    vs_group
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

annotated = (
    deduped
    .select(
        "id",
        "event_at",
        "actor",
        "ticket_opened_at_decile",
        "ticket_opened_at_seq",
        "last_known_event_at",
        "prev3_ticket_opened_at",
        "avg_ticket_opened_at_seq",
        "ticket_opened_at_seq_minus_avg",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev3_ticket_opened_at")).alias("days_since_prev3_ticket_opened_at"),
    )
)

result = (
    annotated
    .select(
        "id",
        "event_at",
        "actor",
        "ticket_opened_at_decile",
        "ticket_opened_at_seq",
        "prev3_ticket_opened_at",
        "avg_ticket_opened_at_seq",
        "ticket_opened_at_seq_minus_avg",
        "days_since_prev3_ticket_opened_at",
        F.round(F.col("event_type_len") / F.nullif(F.col("avg_ticket_opened_at_seq"), F.lit(0)), 3).alias("event_type_len_per_avg_ticket_opened_at_seq"),
    )
    .orderBy(
        F.col("event_type_len_per_avg_ticket_opened_at_seq").asc(),
        F.col("ticket_opened_at_seq_minus_avg").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
