from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_by_duration_min_run = Window.partitionBy("burst_id").orderBy("duration_min", "student_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_min = Window.orderBy(F.col("duration_min").desc(), "student_id", "burst_id")

valid_enrollments = (
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

with_prev = valid_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_student_id_running))

sessions = (
    sessionized
    .groupBy("student_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    sessions
    .select(
        "student_id",
        "burst_id",
        "session_start",
        "session_end",
        "duration_min",
        F.count(F.lit(1)).over(w_burst_id_by_duration_min_run).alias("running_count"),
        F.ntile(3).over(w_all_by_duration_min).alias("duration_min_tercile"),
    )
)

result = (
    with_trends
    .select(
        "student_id",
        "burst_id",
        "running_count",
        "duration_min_tercile",
    )
    .unpivot(
        ids=["student_id", "burst_id"],
        values=["running_count", "duration_min_tercile"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
