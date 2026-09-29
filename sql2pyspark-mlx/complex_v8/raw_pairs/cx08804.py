from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_event_types_by_dist = Window.partitionBy("distinct_event_types").orderBy(F.col("distinct_event_types").desc(), "session_end_ym", "event_type").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_event_types_by_dist_2 = Window.partitionBy("distinct_event_types").orderBy(F.col("distinct_event_types").desc(), "session_end_ym", "event_type").rowsBetween(-3, Window.currentRow)
w_distinct_event_types_by_dist_3 = Window.partitionBy("distinct_event_types").orderBy(F.col("distinct_event_types").desc(), "session_end_ym", "event_type").rowsBetween(-6, -1)

cleaned_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 30)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    sessions
    .filter(F.col("duration_min") > 60.0)
    .groupBy(
        F.date_format("session_end", "yyyy-MM").alias("session_end_ym"),
        "event_type",
    )
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
        F.max("duration_min").alias("max_duration_min"),
        F.min("duration_min").alias("min_duration_min"),
    )
)

windowed = (
    per_group
    .select(
        "*",
        F.max("min_duration_min").over(w_distinct_event_types_by_dist).alias("max_min_duration_min_so_far"),
        F.sum("max_duration_min").over(w_distinct_event_types_by_dist_2).alias("max_duration_min_sum_4"),
        F.sum("n_event_type_is_not_null").over(w_distinct_event_types_by_dist_3).alias("n_event_type_is_not_null_sum_6_prior"),
    )
)

result = (
    windowed
    .select(
        "session_end_ym",
        "n_event_type_is_not_null",
        "max_min_duration_min_so_far",
        "max_duration_min_sum_4",
        "n_event_type_is_not_null_sum_6_prior",
    )
    .orderBy(
        "n_event_type_is_not_null",
        F.col("max_duration_min_sum_4").asc_nulls_last(),
    )
)
