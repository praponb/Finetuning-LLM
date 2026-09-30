from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_enrollments = (
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

with_prev = filtered_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_student_id_running))

result = (
    sessionized
    .rollup("is_new_session", "session_id", "course_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("is_new_session").alias("max_is_new_session"),
        F.sum(F.when(F.col("grade_status") == "FAIL", F.col("session_id"))).alias("session_id_grade_status_fail"),
    )
    .orderBy(
        F.col("is_new_session").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
