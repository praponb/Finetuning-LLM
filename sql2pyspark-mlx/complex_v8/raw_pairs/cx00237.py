from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrollment_grade_status = Window.partitionBy("enrollment_grade_status")
w_enrollment_id_student_id_by = Window.partitionBy("enrollment_id", "student_id").orderBy("enrolled_at")
w_enrollment_id_student_id_by_2 = Window.partitionBy("enrollment_id", "student_id").orderBy("enrolled_at", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id = Window.partitionBy("student_id")
w_enrollment_course_id_enrolle = Window.partitionBy("enrollment_course_id", F.date_trunc("month", "enrolled_at")).orderBy(F.col("score_distance").desc(), "id", "enrollment_id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date")) & (F.col("course_id") <= 11))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.abs(F.col("score") - 38.1).alias("score_distance"),
    )
)

tagged = (
    cleaned_enrollments
    .select(
        "*",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
        F.abs(F.col("score") - 44.4).alias("score_distance_2"),
    )
)

detail_rows = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.score_distance",
        "s.term_ref",
        "s.term_clean",
        "s.score_distance_2",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

windowed = (
    detail_rows
    .select(
        "*",
        F.max("score_distance_2").over(w_enrollment_grade_status).alias("enrollment_grade_status_max_score_distance_2"),
        F.dense_rank().over(w_enrollment_id_student_id_by).alias("enrolled_at_dense_rank"),
        F.max("score_distance").over(w_enrollment_grade_status).alias("enrollment_grade_status_max_score_distance"),
        F.round(F.col("score_distance_2") - F.avg("score_distance_2").over(w_enrollment_grade_status), 2).alias("score_distance_2_vs_enrollment_grade_status_avg"),
        F.nth_value("enrolled_at", 2).over(w_enrollment_id_student_id_by_2).alias("second_enrolled_at"),
    )
)

with_windows = (
    windowed
    .withColumn("student_id_min_score_distance", F.min("score_distance").over(w_student_id))
    .withColumn("next_enrolled_at", F.lead("enrolled_at").over(w_enrollment_course_id_enrolle))
)

result = (
    with_windows
    .groupingSets(
        [["course_id", "enrollment_course_id"], ["course_id"], ["enrollment_course_id"]],
        "course_id", "enrollment_course_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("enrollment_term")).alias("enrollment_term_list"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("enrollment_course_id").asc_nulls_last(),
    )
)
