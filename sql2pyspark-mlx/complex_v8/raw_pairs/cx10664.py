from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_hour = Window.orderBy(F.col("event_at_hour").desc(), "id")
w_all_by_event_at_hour_full = Window.orderBy(F.col("event_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_hour_ties = Window.orderBy(F.col("event_at_hour").desc())

prepared_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.hour(F.col("event_at")).between(9, 11)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour_2"),
    )
)

with_trends = (
    prepared_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_hour_2",
        F.lag("event_at").over(w_all_by_event_at_hour).alias("prev_event_at"),
        F.round((F.unix_timestamp("event_at_hour") - F.unix_timestamp(F.lag("event_at_hour").over(w_all_by_event_at_hour))) / 60, 1).alias("minutes_since_prev_event_at_hour"),
        F.last("event_at").over(w_all_by_event_at_hour_full).alias("last_event_at"),
        F.first("event_at_hour_2").over(w_all_by_event_at_hour_full).alias("first_event_at_hour_2"),
        F.dense_rank().over(w_all_by_event_at_hour_ties).alias("event_at_hour_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_hour_ties), 3).alias("event_at_hour_cume_dist"),
    )
)

first_rows = (
    with_trends
    .filter((F.col("event_at_hour_dense_rank") <= 2) & (F.col("minutes_since_prev_event_at_hour") < 3750.0))
)

result = (
    first_rows
    .rollup("actor", "event_type")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.sum("minutes_since_prev_event_at_hour") / F.nullif(F.sum("event_at_hour_cume_dist"), F.lit(0)), 3).alias("minutes_since_prev_event_at_hour_to_event_at_hou"),
        F.max("event_at_hour_cume_dist").alias("max_event_at_hour_cume_dist"),
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("event_type").asc_nulls_last())
)
