from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_at_to_date_by_ev = Window.partitionBy("actor", F.to_date("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_actor = Window.partitionBy("actor")
w_ticket_id_by_last_event_at_r = Window.partitionBy("ticket_id").orderBy("last_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_event_at_month_by_even = Window.partitionBy(F.date_trunc("month", "first_event_at")).orderBy(F.col("event_at_percent_rank").desc(), "id")
w_ticket_id_by_last_event_at_f = Window.partitionBy("ticket_id").orderBy("last_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_event_at_year_by_first = Window.partitionBy(F.year("next_event_at")).orderBy(F.col("first_event_at_2").asc_nulls_last())

prepared = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

with_details_ticket = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.tags").alias("ticket_tags"))
)

sequenced = (
    with_details_ticket
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_ref",
        "ticket_tags",
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.dense_rank().over(w_ticket_id_by_event_at_ties).alias("event_at_dense_rank"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at_2"),
        F.round(F.percent_rank().over(w_all_by_event_at), 3).alias("event_at_percent_rank"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank_2"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.first("event_at").over(w_actor_event_at_to_date_by_ev).alias("first_event_at_2"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank_2"),
        F.lead("event_at").over(w_all_by_event_at).alias("next_event_at"),
        F.last("event_at").over(w_actor_event_at_to_date_by_ev).alias("last_event_at"),
    )
)

tagged = (
    sequenced
    .filter(F.col("next_event_at") < F.lit("2025-02-12 11:00:00").cast("timestamp"))
    .withColumn("actor_group", F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other"))
)

with_windows = (
    tagged
    .select(
        "id",
        "event_type",
        "actor",
        "actor_ref",
        "ticket_tags",
        "event_at_dense_rank",
        "second_event_at",
        "second_event_at_2",
        "event_at_percent_rank",
        "event_at_percent_rank_2",
        "first_event_at",
        "first_event_at_2",
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
        F.sum("event_at_cume_dist").over(w_ticket_id_by_last_event_at_r).alias("running_event_at_cume_dist"),
        F.rank().over(w_first_event_at_month_by_even).alias("event_at_percent_rank_rank"),
        F.round(F.col("event_at_percent_rank") - F.avg("event_at_percent_rank").over(w_actor), 2).alias("event_at_percent_rank_vs_actor_avg"),
        F.nth_value("event_at_percent_rank", 2).over(w_ticket_id_by_last_event_at_f).alias("second_event_at_percent_rank"),
        F.round(F.percent_rank().over(w_next_event_at_year_by_first), 3).alias("first_event_at_2_percent_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("rows_in_actor") <= 18)
    .select(
        "id",
        "actor",
        "ticket_tags",
        "event_at_dense_rank",
        "second_event_at",
        "second_event_at_2",
        "first_event_at",
        "first_event_at_2",
        "running_event_at_cume_dist",
        "event_at_percent_rank_rank",
        "event_at_percent_rank_vs_actor_avg",
        "first_event_at_2_percent_rank",
        F.array_contains(F.col("ticket_tags"), "login").alias("has_login"),
    )
    .orderBy(F.col("first_event_at").asc_nulls_last())
)
