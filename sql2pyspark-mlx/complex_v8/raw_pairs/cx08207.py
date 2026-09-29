from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .filter(F.col("term") != "2024T1")
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

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_grade_status_running))

result = (
    sessionized
    .groupingSets(
        [["student_id", "is_new_session", "course_id"], ["student_id"]],
        "student_id", "is_new_session", "course_id",
    )
    .agg(
        F.sum(F.when(F.col("term") == "2025T2", F.col("is_new_session"))).alias("is_new_session_term_2025t2"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("is_new_session").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
