from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T2")
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

with_prev = cleaned_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_grade_status_running))

grade_status_stats = (
    sessionized
    .groupBy("grade_status")
    .agg(F.max("new_session_flag").alias("max_new_session_flag"))
)

outliers = (
    sessionized.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.new_session_flag") == F.col("g.max_new_session_flag"))
    .select(
        "s.*",
        "g.max_new_session_flag",
        F.round(F.col("s.new_session_flag") - F.col("g.max_new_session_flag"), 2).alias("new_session_flag_minus_max"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

profiled = (
    outliers.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.last_enrollment_enrolled_at",
    )
)

result = (
    profiled
    .groupingSets(
        [["burst_id", "course_id", "enrollment_count"], ["burst_id"], ["course_id"], []],
        "burst_id", "course_id", "enrollment_count",
    )
    .agg(
        F.count(F.when(F.col("grade_status").isin("PASS", "WITHDRAWN", "INCOMPLETE"), 1)).alias("n_grade_status_in_pass_withdra"),
    )
)
