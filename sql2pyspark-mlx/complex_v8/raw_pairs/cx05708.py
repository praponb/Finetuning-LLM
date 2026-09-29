from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_ticket_agent_id_by_ticket_op = Window.partitionBy("ticket_agent_id").orderBy(F.col("ticket_opened_at").desc())
w_ticket_agent_id_by_ticket_op_2 = Window.partitionBy("ticket_agent_id").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_agent_id_by_ticket_op_3 = Window.partitionBy("ticket_agent_id").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_agent_id_by_ticket_op_4 = Window.partitionBy("ticket_agent_id").orderBy(F.col("ticket_opened_at").desc(), "id")
w_event_at_quintile_by_ticket = Window.partitionBy("event_at_quintile").orderBy("ticket_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_event_at_movin = Window.orderBy(F.col("second_event_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_second_event_at_prior = Window.orderBy(F.col("second_event_at").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_second_event_at = Window.orderBy(F.col("second_event_at").asc_nulls_last(), "id")
w_all_by_event_at_seq_full = Window.orderBy(F.col("event_at_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_quintile_by_ticket_2 = Window.partitionBy("event_at_quintile").orderBy("ticket_opened_at", "id")

valid = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 10) & (F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

enriched_ticket = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

sequenced = (
    enriched_ticket
    .select(
        "id",
        "event_at",
        "event_type",
        "actor_clean",
        "actor_ref",
        "ticket_opened_at",
        "ticket_agent_id",
        "ticket_priority",
        F.ntile(5).over(w_event_type_by_event_at).alias("event_at_quintile"),
        F.rank().over(w_ticket_agent_id_by_ticket_op).alias("ticket_opened_at_rank"),
        F.lag("ticket_opened_at", 3).over(w_event_type_by_event_at).alias("prev3_ticket_opened_at"),
        F.nth_value("event_at", 2).over(w_ticket_agent_id_by_ticket_op_2).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("ticket_opened_at", ignorenulls=True).over(w_ticket_agent_id_by_ticket_op_3).alias("last_known_ticket_opened_at"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.ntile(3).over(w_ticket_agent_id_by_ticket_op_4).alias("ticket_opened_at_tercile"),
        F.dense_rank().over(w_event_type_by_event_at).alias("event_at_dense_rank"),
    )
)

tagged = (
    sequenced
    .withColumn("event_type_or_unknown", F.coalesce(F.col("event_type"), F.lit("UNKNOWN")))
)

with_trends = (
    tagged
    .select(
        "id",
        "event_at",
        "event_type",
        "ticket_opened_at",
        "ticket_agent_id",
        "ticket_priority",
        "event_at_quintile",
        "ticket_opened_at_rank",
        "prev3_ticket_opened_at",
        "minutes_since_prev_event_at",
        "last_known_ticket_opened_at",
        "event_at_seq",
        "ticket_opened_at_tercile",
        "event_at_dense_rank",
        F.first("minutes_since_prev_event_at").over(w_event_at_quintile_by_ticket).alias("first_minutes_since_prev_event_at"),
        F.sum("event_at_seq").over(w_all_by_second_event_at_movin).alias("event_at_seq_sum_2"),
        F.round(F.avg("ticket_opened_at_tercile").over(w_all_by_second_event_at_prior), 2).alias("ticket_opened_at_tercile_avg_3_prior"),
        F.lag("ticket_opened_at_rank").over(w_all_by_second_event_at).alias("prev_ticket_opened_at_rank"),
        F.lead("ticket_opened_at_tercile", 2).over(w_all_by_second_event_at).alias("next2_ticket_opened_at_tercile"),
        F.first("ticket_opened_at_tercile").over(w_all_by_event_at_seq_full).alias("first_ticket_opened_at_tercile"),
        F.row_number().over(w_event_at_quintile_by_ticket_2).alias("rn"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "event_type",
        "ticket_agent_id",
        "ticket_priority",
        "event_at_quintile",
        "ticket_opened_at_rank",
        "prev3_ticket_opened_at",
        "minutes_since_prev_event_at",
        "last_known_ticket_opened_at",
        "event_at_seq",
        "ticket_opened_at_tercile",
        "event_at_dense_rank",
        "first_minutes_since_prev_event_at",
        "event_at_seq_sum_2",
        "ticket_opened_at_tercile_avg_3_prior",
        "prev_ticket_opened_at_rank",
        "next2_ticket_opened_at_tercile",
        "first_ticket_opened_at_tercile",
        "rn",
        (F.floor(F.col("event_at_quintile") / 2) * 2).alias("event_at_quintile_bucket"),
    )
    .orderBy(
        F.col("event_at_dense_rank").asc_nulls_last(),
        F.col("ticket_opened_at_tercile_avg_3_prior").desc(),
        "id",
    )
    .limit(10)
)
