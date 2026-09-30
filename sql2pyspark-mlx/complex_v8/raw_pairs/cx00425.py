from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_plus_7d = Window.orderBy("event_at_plus_7d", "id")
w_all_by_event_at_plus_7d_full = Window.orderBy("event_at_plus_7d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_plus_7d_terc = Window.orderBy(F.col("event_at_plus_7d_tercile").asc_nulls_last(), "id")
w_event_at_plus_7d_dense_rank = Window.partitionBy("event_at_plus_7d_dense_rank")
w_second_event_at_plus_7d_year = Window.partitionBy(F.year("second_event_at_plus_7d")).orderBy(F.col("second_event_at_plus_7d").desc(), "id")
w_all_by_event_at_plus_7d_dens = Window.orderBy(F.col("event_at_plus_7d_dense_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("reopened", "resolved", "assigned")) & (F.month(F.col("event_at")).isin(1, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
    )
)

metrics = (
    cleaned_events
    .select(
        "*",
        F.dense_rank().over(w_all_by_event_at_plus_7d).alias("event_at_plus_7d_dense_rank"),
        F.nth_value("event_at_plus_7d", 2).over(w_all_by_event_at_plus_7d_full).alias("second_event_at_plus_7d"),
        F.ntile(4).over(w_all_by_event_at_plus_7d).alias("event_at_plus_7d_quartile"),
        F.ntile(5).over(w_all_by_event_at_plus_7d).alias("event_at_plus_7d_quintile"),
        F.ntile(3).over(w_all_by_event_at_plus_7d).alias("event_at_plus_7d_tercile"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.lead("event_at_month").over(w_all_by_event_at_plus_7d_terc).alias("next_event_at_month"),
        F.min("event_at_plus_7d_quartile").over(w_event_at_plus_7d_dense_rank).alias("event_at_plus_7d_dense_rank_min_event_at_plus_7d"),
        F.max("event_at_plus_7d_tercile").over(w_event_at_plus_7d_dense_rank).alias("event_at_plus_7d_dense_rank_max_event_at_plus_7d"),
        F.rank().over(w_second_event_at_plus_7d_year).alias("second_event_at_plus_7d_rank"),
        F.last("event_at_hour", ignorenulls=True).over(w_all_by_event_at_plus_7d_dens).alias("last_known_event_at_hour"),
        F.round(F.avg("event_at_plus_7d_quintile").over(w_event_at_plus_7d_dense_rank), 2).alias("event_at_plus_7d_dense_rank_avg_event_at_plus_7d"),
    )
)

enriched = (
    with_trends
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_hour",
        "event_at_plus_7d",
        "event_at_plus_7d_dense_rank",
        "event_at_plus_7d_quintile",
        "next_event_at_month",
        "event_at_plus_7d_dense_rank_min_event_at_plus_7d",
        "event_at_plus_7d_dense_rank_max_event_at_plus_7d",
        "second_event_at_plus_7d_rank",
        "last_known_event_at_hour",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_type",
        "event_at_hour",
        "event_at_plus_7d_dense_rank",
        "event_at_plus_7d_quintile",
        "event_at_plus_7d_dense_rank_max_event_at_plus_7d",
        "second_event_at_plus_7d_rank",
        "event_type_actor_label",
        F.length(F.trim(F.col("event_type_slug"))).alias("event_type_slug_len"),
        F.to_date(F.col("next_event_at_month")).alias("next_event_at_month_date"),
    )
    .orderBy(
        F.col("event_type_actor_label").desc(),
        F.col("next_event_at_month_date").desc(),
    )
)
