from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
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

with_prev = filtered.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_term_running))

per_session = (
    sessionized
    .groupBy("term", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

totals = (
    per_session
    .groupBy(
        F.date_trunc("month", "session_start").alias("session_start_month"),
        "term",
        "session_id",
    )
    .agg(
        F.max("session_start").alias("last_session_start"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.max("duration_min").alias("max_duration_min_check"),
    )
    .filter(F.col("max_duration_min_check") > 48.0)
    .drop("max_duration_min_check")
)

result = (
    totals
    .rollup("term", "session_id")
    .agg(
        F.countDistinct("term").alias("distinct_terms"),
        F.round(F.stddev("session_id"), 2).alias("stddev_session_id"),
    )
    .orderBy(F.col("term").asc_nulls_last(), F.col("session_id").asc_nulls_last())
)
