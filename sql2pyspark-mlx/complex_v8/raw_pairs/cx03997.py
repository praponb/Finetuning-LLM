from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_full_2 = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_runnin = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_dedup = Window.partitionBy("event_type", "event_type_group").orderBy(F.col("first_known_event_at").desc(), F.col("id").desc())

prepared_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("replied", "resolved"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
    )
)

ranked = (
    prepared_events
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full_2).alias("second_event_at_2"),
        F.first("event_at", ignorenulls=True).over(w_ticket_id_by_event_at_runnin).alias("first_known_event_at"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_stats = (
    ranked.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

deduped = (
    with_stats
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "second_event_at",
        "second_event_at_2",
        "first_known_event_at",
        "event_at_rank",
        "first_event_at",
        F.to_date(F.col("first_event_at")).alias("first_event_at_date"),
    )
)
