from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy("hours_since_open", "id")
w_n_priority_is_not_null_event = Window.partitionBy("n_priority_is_not_null", "event_type")
w_all_by_hours_since_open_prio = Window.orderBy(F.col("hours_since_open_priority_p4").asc_nulls_last(), "event_type", "priority")
w_all_by_max_hours_since_open = Window.orderBy("max_hours_since_open", "event_type", "priority")
w_all_by_max_hours_since_open_2 = Window.orderBy("max_hours_since_open", "event_type", "priority").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

latest = (
    matched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

per_group = (
    latest
    .filter(F.col("hours_since_open").between(10.8, 57.6))
    .groupBy("event_type", "priority")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.sum(F.when(F.col("priority").isNotNull(), 1).otherwise(0)).alias("n_priority_is_not_null"),
        F.sum(F.when(F.col("priority") != "P4", F.col("hours_since_open")).otherwise(0)).alias("hours_since_open_priority_p4"),
        F.max("hours_since_open").alias("max_hours_since_open"),
    )
)

metrics = (
    per_group
    .select(
        "*",
        F.count(F.lit(1)).over(w_n_priority_is_not_null_event).alias("rows_in_n_priority_is_not_null_event_type"),
        F.round(F.col("n_priority_is_not_null") - F.avg("n_priority_is_not_null").over(w_n_priority_is_not_null_event), 2).alias("n_priority_is_not_null_vs_n_priority_is_not_null"),
        F.ntile(3).over(w_all_by_hours_since_open_prio).alias("hours_since_open_priority_p4_tercile"),
        (F.col("hours_since_open_priority_p4") - F.lag("hours_since_open_priority_p4").over(w_all_by_max_hours_since_open)).alias("hours_since_open_priority_p4_change"),
        F.count(F.lit(1)).over(w_all_by_max_hours_since_open_2).alias("running_count"),
    )
)

result = (
    metrics
    .select(
        "event_type",
        "priority",
        "event_type_list",
        "hours_since_open_priority_p4",
        "max_hours_since_open",
        "rows_in_n_priority_is_not_null_event_type",
        "n_priority_is_not_null_vs_n_priority_is_not_null",
        "hours_since_open_priority_p4_tercile",
        "hours_since_open_priority_p4_change",
        "running_count",
        (F.col("max_hours_since_open") - F.col("cnt")).alias("max_hours_since_open_minus_cnt"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("cnt").cast("string")).alias("event_type_ref"),
    )
    .orderBy(
        F.col("hours_since_open_priority_p4_tercile").asc(),
        "event_type",
        "priority",
    )
    .limit(5)
)
