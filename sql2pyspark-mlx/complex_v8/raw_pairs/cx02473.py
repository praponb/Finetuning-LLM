from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").asc_nulls_last())
w_latest = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at_percent_rank").desc(), F.col("id").desc())
w_all_by_event_at_moving_2 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_event_type = Window.partitionBy("event_type")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_prior_1 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(-1, -1)

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

with_windows = (
    base_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        F.nth_value("event_at", 2).over(w_ticket_id_actor_by_event_at).alias("second_event_at"),
        F.round(F.percent_rank().over(w_ticket_id_actor_by_event_at_2), 3).alias("event_at_percent_rank"),
    )
)

latest_per_key = (
    with_windows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    latest_per_key
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "second_event_at",
        "event_at_percent_rank",
        F.round(F.avg("event_at_percent_rank").over(w_all_by_event_at_moving_2), 2).alias("event_at_percent_rank_avg_3"),
        F.min("event_at_percent_rank").over(w_event_type).alias("event_type_min_event_at_percent_rank"),
        F.round((F.unix_timestamp("second_event_at") - F.unix_timestamp(F.lag("second_event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_second_event_at"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.max("event_at_percent_rank").over(w_all_by_event_at_prior_1).alias("event_at_percent_rank_max_1_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "second_event_at",
        "event_at_percent_rank",
        "event_at_percent_rank_avg_3",
        "event_type_min_event_at_percent_rank",
        "minutes_since_prev_second_event_at",
        "rows_in_event_type",
        "event_at_percent_rank_max_1_prior",
        F.least(F.col("rows_in_event_type"), F.col("event_at_percent_rank")).alias("least_rows_in_event_type_event_at_percent_rank"),
    )
    .orderBy(F.col("rows_in_event_type").desc(), "id")
    .limit(5)
)
