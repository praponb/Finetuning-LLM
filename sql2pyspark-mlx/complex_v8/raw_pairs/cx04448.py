from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_event_at_month_ticket_id_by = Window.partitionBy(F.date_trunc("month", "event_at"), "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_ticket_id_by_2 = Window.partitionBy(F.date_trunc("month", "event_at"), "ticket_id").orderBy(F.col("event_at").asc_nulls_last())

recent_events = (
    df_ticket_events
    .filter((F.col("actor").isin("system", "agent")) & (F.col("event_type").isin("replied", "assigned")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

unique_rows = (
    recent_events
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sequenced = (
    unique_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.nth_value("event_at", 2).over(w_event_at_month_ticket_id_by).alias("second_event_at"),
        F.rank().over(w_event_at_month_ticket_id_by_2).alias("event_at_rank"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "agent"), "left_anti")
)

totals = (
    filtered
    .groupBy(
        "event_type",
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "ticket_id",
    )
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.sum("event_at_rank").alias("total_event_at_rank"),
    )
)

result = (
    totals
    .filter(F.hour(F.col("event_at_month")).between(10, 16))
    .select(
        "event_type",
        "event_at_month",
        "ticket_id",
        "event_type_list",
    )
    .orderBy(
        F.col("event_at_month").desc_nulls_first(),
        F.col("ticket_id").desc_nulls_first(),
        "event_type",
    )
    .limit(10)
)
