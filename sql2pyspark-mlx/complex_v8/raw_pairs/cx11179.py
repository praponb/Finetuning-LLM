from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_event_type_event_at_tercile = Window.partitionBy("event_type", "event_at_tercile")
w_all_by_max_rows_in_event_typ = Window.orderBy("max_rows_in_event_type_event_at_tercile")
w_distinct_event_types_by_last = Window.partitionBy("distinct_event_types").orderBy(F.col("last_second_event_at").desc())

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    scoped_events
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.ntile(3).over(w_actor_by_event_at).alias("event_at_tercile"),
    )
)

annotated = (
    with_windows
    .select(
        "*",
        (F.floor(F.col("event_at_tercile") / 2) * 2).alias("event_at_tercile_bucket"),
        (F.floor(F.col("event_at_tercile") / 2) * 2).alias("event_at_tercile_bucket_2"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "event_at",
        "event_type",
        "second_event_at",
        F.count(F.lit(1)).over(w_event_type_event_at_tercile).alias("rows_in_event_type_event_at_tercile"),
        F.round(F.col("event_at_tercile_bucket_2") - F.avg("event_at_tercile_bucket_2").over(w_event_type_event_at_tercile), 2).alias("event_at_tercile_bucket_2_vs_event_type_event_at"),
    )
)

stats = (
    ranked
    .groupBy(F.to_date("second_event_at").alias("second_event_at_day"))
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.max("second_event_at").alias("last_second_event_at"),
        F.max("rows_in_event_type_event_at_tercile").alias("max_rows_in_event_type_event_at_tercile"),
    )
)

ranked_2 = (
    stats
    .select(
        "second_event_at_day",
        "distinct_event_types",
        "last_second_event_at",
        F.round(F.percent_rank().over(w_all_by_max_rows_in_event_typ), 3).alias("max_rows_in_event_type_event_at_tercile_percent_"),
        F.round(F.cume_dist().over(w_distinct_event_types_by_last), 3).alias("last_second_event_at_cume_dist"),
    )
)

without_matches = (
    ranked_2
    .crossJoin(ranked_2.agg(F.avg("max_rows_in_event_type_event_at_tercile_percent_").alias("overall_avg_max_rows_in_event_type_event_at_terc")))
    .filter(F.col("max_rows_in_event_type_event_at_tercile_percent_") > F.col("overall_avg_max_rows_in_event_type_event_at_terc"))
    .drop("overall_avg_max_rows_in_event_type_event_at_terc")
)

result = (
    without_matches
    .select(
        "second_event_at_day",
        "distinct_event_types",
        F.to_date(F.col("last_second_event_at")).alias("last_second_event_at_date"),
    )
    .orderBy(
        F.col("distinct_event_types").desc(),
        F.col("second_event_at_day").desc(),
    )
    .limit(10)
)
