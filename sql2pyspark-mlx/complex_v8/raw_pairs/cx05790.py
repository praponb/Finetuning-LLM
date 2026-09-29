from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("event_type", "event_actor").orderBy("last_second_event_at", "ticket_id", "actor", "event_id")

recent = (
    df_ticket_events
    .filter((F.col("event_type") != "escalated") & (F.col("ticket_id") <= 51))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    recent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lag("event_at", 2).over(w_event_type_by_event_at).alias("prev2_event_at"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.dense_rank().over(w_event_type_by_event_at).alias("event_at_dense_rank"),
    )
)

grouped = (
    with_windows
    .groupBy("ticket_id", "actor")
    .agg(
        F.count(F.when(F.col("actor") == "customer", 1)).alias("n_actor_customer"),
        F.min("event_at_dense_rank").alias("min_event_at_dense_rank"),
        F.max("second_event_at").alias("last_second_event_at"),
    )
)

detail_rows = (
    grouped.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.ticket_id",
        "s.actor",
        "s.n_actor_customer",
        "s.min_event_at_dense_rank",
        "s.last_second_event_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

latest_per_key = (
    detail_rows
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_parent_event = (
    latest_per_key.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.id") == F.col("s.event_id")) & (~F.col("t.event_type").isin("created", "reopened")), "left")
    .select(
        "s.ticket_id",
        "s.actor",
        "s.n_actor_customer",
        "s.min_event_at_dense_rank",
        "s.event_id",
        "s.event_actor",
        "s.event_type",
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor_2"),
    )
)

result = (
    with_parent_event
    .rollup("event_type", "actor")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("n_actor_customer").alias("total_n_actor_customer"),
    )
    .orderBy(F.col("event_type").asc_nulls_last(), F.col("actor").asc_nulls_last())
)
