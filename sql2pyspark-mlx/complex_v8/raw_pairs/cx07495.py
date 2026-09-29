from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status").orderBy("enrolled_at", "id")
w_grade_status_running = Window.partitionBy("grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_enrollments
    .filter(F.col("grade_status") != "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

with_prev = prepared.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_grade_status))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_grade_status_running))

with_details = (
    sessionized.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept").isin("Math", "Biz"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_date",
        "s.prev_enrolled_at",
        "s.burst_id",
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    with_details
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at_date",
        "prev_enrolled_at",
        "course_dept",
        F.year(F.col("enrolled_at_date")).alias("enrolled_at_date_year"),
        (F.col("burst_id") - F.col("score")).alias("burst_id_minus_score"),
    )
)
