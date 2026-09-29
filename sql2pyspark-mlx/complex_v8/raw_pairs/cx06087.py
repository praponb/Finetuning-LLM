from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_actor_event_type_by_actor_le = Window.partitionBy("actor", "event_type").orderBy(F.col("actor_len").desc(), "id").rowsBetween(-2, -1)
w_event_type_actor_by_ticket_r = Window.partitionBy("event_type", "actor").orderBy(F.col("ticket_requester_id").desc(), "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_event_type_actor_by_ticket_r_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_actor_le_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("actor_len").desc(), "id")
w_event_at_month_ticket_id_by = Window.partitionBy(F.date_trunc("month", "event_at"), "ticket_id").orderBy("actor_len", "id").rowsBetween(-1, Window.currentRow)
w_actor_len_event_type_by_acto = Window.partitionBy("actor_len", "event_type").orderBy("actor_len")
w_distinct_actor_in_event_type = Window.partitionBy("distinct_actor_in_event_type", "first_actor_len")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_actor_distinct_actor_in_even = Window.partitionBy("actor", "distinct_actor_in_event_type").orderBy(F.col("actor_len_share_of_event_type").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_len_event_at_year_by_a = Window.partitionBy("actor_len", F.year("event_at")).orderBy(F.col("actor_len").desc(), "id")
w_all_by_event_at_prior_2 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_ticket_requester_id = Window.orderBy(F.col("ticket_requester_id").desc_nulls_first(), "id")
w_distinct_actor_in_event_type_2 = Window.partitionBy("distinct_actor_in_event_type")

prepared = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.requester_id") >= 6)
    .select("s.*", F.col("t.requester_id").alias("ticket_requester_id"))
)

with_windows = (
    with_parent
    .select(
        "*",
        F.size(F.collect_set("actor").over(w_event_type)).alias("distinct_actor_in_event_type"),
        F.round(F.avg("ticket_requester_id").over(w_actor_event_type_by_actor_le), 2).alias("ticket_requester_id_avg_2_prior"),
        F.lead("event_at", 3).over(w_event_type_actor_by_ticket_r).alias("next3_event_at"),
        F.round(F.col("actor_len") / F.nullif(F.sum("actor_len").over(w_ticket_id), F.lit(0)), 4).alias("actor_len_share_of_ticket_id"),
        F.first("actor_len").over(w_event_type_actor_by_ticket_r_2).alias("first_actor_len"),
        (F.col("ticket_requester_id") - F.lag("ticket_requester_id").over(w_actor_event_type_by_actor_le_2)).alias("ticket_requester_id_change"),
        F.round(F.col("actor_len") / F.nullif(F.sum("actor_len").over(w_event_type), F.lit(0)), 4).alias("actor_len_share_of_event_type"),
        F.max("ticket_requester_id").over(w_event_at_month_ticket_id_by).alias("ticket_requester_id_max_2"),
        F.max("ticket_requester_id").over(w_event_type).alias("event_type_max_ticket_requester_id"),
        F.round(F.cume_dist().over(w_actor_len_event_type_by_acto), 3).alias("actor_len_cume_dist"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.max("first_actor_len").over(w_distinct_actor_in_event_type).alias("distinct_actor_in_event_type_first_actor_len_max"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.nth_value("actor_len_cume_dist", 2).over(w_actor_distinct_actor_in_even).alias("second_actor_len_cume_dist"),
        F.ntile(3).over(w_actor_len_event_at_year_by_a).alias("actor_len_tercile"),
        F.round(F.avg("distinct_actor_in_event_type").over(w_all_by_event_at_prior_2), 2).alias("distinct_actor_in_event_type_avg_2_prior"),
        (F.col("first_actor_len") - F.lag("first_actor_len").over(w_all_by_ticket_requester_id)).alias("first_actor_len_change"),
        F.round(F.col("first_actor_len") - F.avg("first_actor_len").over(w_distinct_actor_in_event_type_2), 2).alias("first_actor_len_vs_distinct_actor_in_event_type_"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_len",
        "distinct_actor_in_event_type",
        "next3_event_at",
        "actor_len_share_of_ticket_id",
        "first_actor_len",
        "ticket_requester_id_change",
        "ticket_requester_id_max_2",
        "event_type_max_ticket_requester_id",
        "actor_len_cume_dist",
        "distinct_actor_in_event_type_first_actor_len_max",
        "seq_no",
        "second_actor_len_cume_dist",
        "actor_len_tercile",
        "distinct_actor_in_event_type_avg_2_prior",
        "first_actor_len_change",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
    )
)
