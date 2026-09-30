from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_priority_p3 = Window.partitionBy("n_priority_p3")
w_ticket_count_agent_id_by_tic = Window.partitionBy("ticket_count", "agent_id").orderBy("ticket_requester_id", "csat", "priority", "agent_id", "ticket_id")
w_priority_by_total_requester = Window.partitionBy("priority").orderBy(F.col("total_requester_id").desc_nulls_first(), "csat", "priority", "agent_id", "ticket_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_total_requester_2 = Window.partitionBy("priority").orderBy(F.col("total_requester_id").desc_nulls_first(), "csat", "priority", "agent_id", "ticket_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_count = Window.partitionBy("ticket_count")
w_csat_prev_ticket_count_by_ti = Window.partitionBy("csat", "prev_ticket_count").orderBy(F.col("ticket_requester_id").asc_nulls_last(), "csat", "priority", "agent_id", "ticket_id")

prepared_tickets = (
    df_tickets
    .filter(F.col("agent_id") >= 6)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

rollup_base = (
    prepared_tickets
    .groupBy("csat", "priority", "agent_id")
    .agg(
        F.sum("requester_id").alias("total_requester_id"),
        F.sum(F.when(F.col("priority") == "P3", 1).otherwise(0)).alias("n_priority_p3"),
        F.count("*").alias("cnt"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("requester_id_to_csat_ratio"),
    )
)

screened = (
    rollup_base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
    .filter(F.col("cnt") <= 8)
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
        F.count("*").alias("ticket_count_3"),
    )
)

profiled = (
    screened.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
        F.coalesce(F.col("t.ticket_count_3"), F.lit(0)).alias("ticket_count_3"),
    )
)

detail_rows = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "inner")
    .select(
        "s.csat",
        "s.priority",
        "s.agent_id",
        "s.total_requester_id",
        "s.n_priority_p3",
        "s.ticket_count",
        "s.ticket_count_2",
        F.col("t.id").alias("ticket_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

sequenced = (
    detail_rows
    .select(
        "csat",
        "priority",
        "agent_id",
        "total_requester_id",
        "n_priority_p3",
        "ticket_count",
        "ticket_count_2",
        "ticket_id",
        "ticket_requester_id",
        F.count(F.lit(1)).over(w_n_priority_p3).alias("rows_in_n_priority_p3"),
        F.ntile(10).over(w_ticket_count_agent_id_by_tic).alias("ticket_requester_id_decile"),
        F.first("ticket_requester_id", ignorenulls=True).over(w_priority_by_total_requester).alias("first_known_ticket_requester_id"),
        F.lag("ticket_count", 1, 0).over(w_ticket_count_agent_id_by_tic).alias("prev_ticket_count"),
        F.first("total_requester_id", ignorenulls=True).over(w_priority_by_total_requester).alias("first_known_total_requester_id"),
        F.first("ticket_requester_id").over(w_priority_by_total_requester_2).alias("first_ticket_requester_id"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.max("prev_ticket_count").over(w_ticket_count).alias("ticket_count_max_prev_ticket_count"),
        F.ntile(3).over(w_csat_prev_ticket_count_by_ti).alias("ticket_requester_id_tercile"),
        F.dense_rank().over(w_csat_prev_ticket_count_by_ti).alias("ticket_requester_id_dense_rank"),
    )
)

result = (
    metrics
    .filter(F.col("ticket_count_2") < 6)
    .select(
        "csat",
        "priority",
        "total_requester_id",
        "ticket_count",
        "ticket_id",
        "rows_in_n_priority_p3",
        "ticket_requester_id_decile",
        "first_known_ticket_requester_id",
        "prev_ticket_count",
        "first_known_total_requester_id",
        "first_ticket_requester_id",
        "ticket_count_max_prev_ticket_count",
        "ticket_requester_id_tercile",
        "ticket_requester_id_dense_rank",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)
