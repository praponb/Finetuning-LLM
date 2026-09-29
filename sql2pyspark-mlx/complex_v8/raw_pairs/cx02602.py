from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at_da = Window.partitionBy("student_id").orderBy(F.col("enrolled_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_student_id_by_enrolled_at_da).alias("running_count"),
        F.sum("score").over(w_student_id_by_enrolled_at_da).alias("running_score"),
    )
)

with_children = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_dayofweek",
        "s.running_count",
        "s.running_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

summarized_rows = (
    with_children.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

result = (
    summarized_rows_2
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "running_count",
        "running_score",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_score",
        "avg_enrollment_score",
    )
    .orderBy(F.col("enrollment_grade_status").desc(), "id", "enrollment_id")
    .limit(5)
)
