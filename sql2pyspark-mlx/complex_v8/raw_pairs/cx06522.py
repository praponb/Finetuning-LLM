from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_term_running))

sessions = (
    sessionized
    .groupBy("term", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    sessions
    .groupBy("event_count", "session_no")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("event_count").alias("total_event_count"),
        F.countDistinct("term").alias("distinct_terms"),
        F.round(F.avg("event_count"), 2).alias("avg_event_count"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum("duration_min").alias("sum_duration_min_check"),
    )
    .filter(F.col("sum_duration_min_check") > 72.0)
    .drop("sum_duration_min_check")
)

scored = per_group.withColumn("avg_event_count_adj", F.round(F.col("avg_event_count") * 1.07, 2))

result = (
    scored
    .rollup("cnt", "session_no")
    .agg(
        F.grouping("cnt").alias("is_total_cnt"),
        F.percentile_approx("avg_event_count_adj", 0.5).alias("median_avg_event_count_adj"),
        F.max("avg_event_count").alias("max_avg_event_count"),
    )
)
