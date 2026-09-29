from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_year_by_event_at = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_at_year_by_event_at_ti = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_at").desc())
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at")
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc_nulls_first())
w_all_by_event_at_2 = Window.orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at_running = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.round(F.cume_dist().over(w_event_at_year_by_event_at), 3).alias("event_at_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at), 3).alias("event_at_percent_rank_2"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.dense_rank().over(w_event_at_year_by_event_at_ti).alias("event_at_dense_rank"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at_2), 3).alias("event_at_percent_rank_3"),
        F.round(F.cume_dist().over(w_actor_event_type_by_event_at_3), 3).alias("event_at_cume_dist_2"),
        F.round(F.percent_rank().over(w_event_at_year_by_event_at), 3).alias("event_at_percent_rank_4"),
        F.round(F.cume_dist().over(w_all_by_event_at_2), 3).alias("event_at_cume_dist_3"),
        F.round(F.percent_rank().over(w_all_by_event_at_2), 3).alias("event_at_percent_rank_5"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.dense_rank().over(w_actor_event_type_by_event_at_2).alias("event_at_dense_rank_2"),
    )
)

scored = (
    windowed
    .filter(F.col("actor").isNotNull())
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.date_trunc("year", F.col("prev_event_at")).alias("prev_event_at_year"),
    )
)

result = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "actor_prefix",
        "event_at_cume_dist",
        "first_event_at",
        "event_at_percent_rank_2",
        "prev_event_at",
        "event_at_dense_rank",
        "event_at_percent_rank_3",
        "event_at_cume_dist_2",
        "event_at_percent_rank_5",
        "event_at_dense_rank_2",
        "prev_event_at_year",
        F.month(F.col("first_known_event_at")).alias("first_known_event_at_month"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy(F.col("first_event_at").desc())
)
