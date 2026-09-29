from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_by_session_end = Window.partitionBy("course_id").orderBy("session_end", "course_id", "session_id")
w_session_end_year = Window.partitionBy(F.year("session_end"))

scoped = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
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

with_prev = scoped.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_course_id_running))

sessions = (
    sessionized
    .groupBy("course_id", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    sessions
    .select(
        "course_id",
        "session_id",
        "event_count",
        "duration_min",
        F.rank().over(w_course_id_by_session_end).alias("session_end_rank"),
        F.count(F.lit(1)).over(w_session_end_year).alias("rows_in_session_end_year"),
        F.round(F.col("duration_min") - F.avg("duration_min").over(w_session_end_year), 2).alias("duration_min_vs_session_end_year_avg"),
        F.dense_rank().over(w_course_id_by_session_end).alias("session_end_dense_rank"),
        (F.col("session_id") - F.lag("session_id").over(w_course_id_by_session_end)).alias("session_id_change"),
        F.round(F.cume_dist().over(w_course_id_by_session_end), 3).alias("session_end_cume_dist"),
    )
)

result = (
    metrics
    .filter(F.col("session_id_change") < 4)
    .select(
        "course_id",
        "session_id",
        "event_count",
        "session_end_rank",
        "rows_in_session_end_year",
        "duration_min_vs_session_end_year_avg",
        "session_end_dense_rank",
        "session_id_change",
        "session_end_cume_dist",
        F.coalesce(F.col("event_count"), F.lit(0)).alias("event_count_filled"),
    )
    .orderBy(
        F.col("rows_in_session_end_year").desc(),
        "duration_min_vs_session_end_year_avg",
        "course_id",
        "session_id",
    )
    .limit(5)
)
