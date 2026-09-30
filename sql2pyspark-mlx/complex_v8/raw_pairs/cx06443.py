from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id")
w_course_id_by_enrolled_at_ful = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_term_by_enrolled = Window.partitionBy("student_id", "term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_course_id_by_enrolled_at_run = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_score_prior_2 = Window.orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-2, -1)

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term").isNotNull())
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

windowed = (
    filtered_enrollments
    .select(
        "*",
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.nth_value("score", 2).over(w_course_id_by_enrolled_at_ful).alias("second_score"),
        F.max("score").over(w_student_id_term_by_enrolled).alias("score_max_2"),
        F.max("score").over(w_course_id_by_enrolled_at_run).alias("max_score_so_far"),
        F.round(F.avg("score").over(w_all_by_score_prior_2), 2).alias("score_avg_2_prior"),
        F.nth_value("enrolled_at", 2).over(w_course_id_by_enrolled_at_ful).alias("second_enrolled_at"),
    )
)

exploded_detail = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.max_score_so_far",
        "s.score_avg_2_prior",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "student_id",
        "course_id",
        "max_score_so_far",
        "score_avg_2_prior",
        "enrollment_id",
        "enrollment_term",
        "enrollment_course_id",
        F.concat(F.col("enrollment_term"), F.lit("#"), F.col("enrollment_id").cast("string")).alias("enrollment_term_ref"),
    )
    .orderBy(F.col("enrollment_term_ref").desc(), "id", "enrollment_id")
    .limit(5)
)
