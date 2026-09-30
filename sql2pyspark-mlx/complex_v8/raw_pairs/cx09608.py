from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_ticket_closed = Window.partitionBy("event_type").orderBy("ticket_closed_at")
w_event_type_by_ticket_closed_2 = Window.partitionBy("event_type").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_ticket_closed_3 = Window.partitionBy("event_type").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_date_f = Window.partitionBy("ticket_id").orderBy(F.col("event_at_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_date_ties = Window.orderBy(F.col("event_at_date").desc())
w_all_by_event_at_date = Window.orderBy(F.col("event_at_date").desc(), "id")
w_n_event_type_in_escalated_cr = Window.partitionBy("n_event_type_in_escalated_crea").orderBy("pct_event_type_replied", "second_event_at_date_month")
w_n_event_type_in_escalated_cr_2 = Window.partitionBy("n_event_type_in_escalated_crea").orderBy("pct_event_type_replied", "second_event_at_date_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_date",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.round(F.cume_dist().over(w_event_type_by_ticket_closed), 3).alias("ticket_closed_at_cume_dist"),
        F.last("event_at").over(w_event_type_by_ticket_closed_2).alias("last_event_at"),
        F.first("event_at_date", ignorenulls=True).over(w_event_type_by_ticket_closed_3).alias("first_known_event_at_date"),
        F.last("event_at_date").over(w_ticket_id_by_event_at_date_f).alias("last_event_at_date"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at_2"),
        F.nth_value("event_at_date", 2).over(w_all_by_event_at_full).alias("second_event_at_date"),
        F.round(F.cume_dist().over(w_all_by_event_at_date_ties), 3).alias("event_at_date_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_event_at_date), 3).alias("event_at_date_cume_dist_2"),
        F.rank().over(w_all_by_event_at_date_ties).alias("event_at_date_rank"),
    )
)

stats = (
    metrics
    .groupBy(
        F.date_trunc("month", "second_event_at_date").alias("second_event_at_date_month"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "escalated", F.col("event_at_date_cume_dist_2"))).alias("event_at_date_cume_dist_2_event_type_escalated"),
        F.round(100 * F.count(F.when(F.col("event_type") != "replied", 1)) / F.count("*"), 1).alias("pct_event_type_replied"),
        F.round(F.stddev("event_at_date_cume_dist_2"), 2).alias("stddev_event_at_date_cume_dist_2"),
        F.count(F.when(F.col("event_type").isin("escalated", "created", "assigned"), 1)).alias("n_event_type_in_escalated_crea"),
    )
)

sequenced = (
    stats
    .select(
        "second_event_at_date_month",
        "stddev_event_at_date_cume_dist_2",
        "n_event_type_in_escalated_crea",
        F.ntile(4).over(w_n_event_type_in_escalated_cr).alias("pct_event_type_replied_quartile"),
        F.sum("pct_event_type_replied").over(w_n_event_type_in_escalated_cr_2).alias("running_pct_event_type_replied"),
    )
)

result = (
    sequenced
    .filter(F.col("second_event_at_date_month") >= F.lit("2025-01-09 09:00:00").cast("timestamp"))
    .select(
        "second_event_at_date_month",
        "n_event_type_in_escalated_crea",
        "pct_event_type_replied_quartile",
        "running_pct_event_type_replied",
    )
    .orderBy(F.col("n_event_type_in_escalated_crea").desc())
)
